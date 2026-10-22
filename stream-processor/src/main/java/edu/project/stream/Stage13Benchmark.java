package edu.project.stream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.*;

/** Repeats existing Stage 6-12 experiments in isolated JVMs; never fabricates measurements. */
public final class Stage13Benchmark {
    static final Path OUT = Path.of("stage13-results");
    static final String[] EXPERIMENTS = {"StragglerExperiment", "StragglerDetectionExperiment", "BarrierAlignmentExperiment", "DurableMultiInputRecoveryExperiment", "CheckpointFailureExperiment"};
    record Sample(String experiment, String scenario, String metric, double value, int trial) {}
    static final List<Sample> samples = new ArrayList<>();
    static final List<String> runs = new ArrayList<>();
    static String javaExe() { return Path.of(System.getProperty("java.home"), "bin", "java").toString(); }
    static String csv(String s) { return "\"" + s.replace("\"", "\"\"") + "\""; }
    static void add(String exp, String scenario, String metric, String value, int trial) {
        samples.add(new Sample(exp, scenario, metric, Double.parseDouble(value), trial));
    }
    static String extract(String output, String regex) {
        Matcher m = Pattern.compile(regex, Pattern.MULTILINE).matcher(output);
        if (!m.find()) throw new IllegalStateException("Expected metric missing: " + regex);
        return m.group(1);
    }
    static void parse(String exp, String out, int trial) {
        switch (exp) {
            case "StragglerExperiment" -> {
                for (String scenario : List.of("BASELINE", "MODERATE", "SEVERE", "STALL")) {
                    Matcher m = Pattern.compile("(?m)^"+scenario+"\\s+([\\d.]+)\\s+([\\d.]+)\\s+([\\d.]+)\\s+([\\d.]+)\\s+([\\d.]+)\\s+PASS\\s*$").matcher(out);
                    if (!m.find()) throw new IllegalStateException("Stage 6 row missing " + scenario);
                    String[] names={"elapsed_ms", "throughput_events_sec", "avg_checkpoint_ms", "checkpoint20_ms", "checkpoint30_ms"};
                    for (int i=0;i<names.length;i++) add(exp,scenario,names[i],m.group(i+1),trial);
                }
            }
            case "StragglerDetectionExperiment" -> {
                String[] scenarios={"MONITOR_ONLY","ADAPTIVE_FLOW_CONTROL"};
                String[][] metrics={{"Elapsed \\(ms\\)","elapsed_ms"},{"Throughput \\(events/s\\)","throughput_events_sec"},{"Avg event latency \\(ms\\)","avg_event_latency_ms"},{"Max event latency \\(ms\\)","max_event_latency_ms"},{"Peak outstanding events","peak_outstanding"}};
                for (String[] metric:metrics) {
                    Matcher m=Pattern.compile("(?m)^"+metric[0]+"\\s+([\\d.]+)\\s+([\\d.]+)\\s*$").matcher(out);
                    if(!m.find()) throw new IllegalStateException("Stage 7 metric missing: "+metric[1]);
                    add(exp,scenarios[0],metric[1],m.group(1),trial);
                    add(exp,scenarios[1],metric[1],m.group(2),trial);
                }
                if(!Pattern.compile("(?m)^Correctness\\s+PASS\\s+PASS\\s*$").matcher(out).find()) throw new IllegalStateException("Stage 7 correctness failed");
            }
            case "BarrierAlignmentExperiment" -> {
                String[][] metrics={{"Barrier span \\(ms\\)","barrier_span_ms"},{"Max buffered events","max_buffered"},{"In-flight records captured","channel_records"},{"Events processed","processed_events"}};
                for(String[] metric:metrics) {
                    Matcher m=Pattern.compile("(?m)^"+metric[0]+"\\s+([\\d.]+)\\s+([\\d.]+)\\s*$").matcher(out);
                    if(!m.find()) throw new IllegalStateException("Stage 8 metric missing: "+metric[1]);
                    add(exp,"ALIGNED",metric[1],m.group(1),trial);
                    add(exp,"UNALIGNED",metric[1],m.group(2),trial);
                }
                if(!Pattern.compile("(?m)^Correctness\\s+PASS\\s+PASS\\s*$").matcher(out).find()) throw new IllegalStateException("Stage 8 correctness failed");
            }
            case "DurableMultiInputRecoveryExperiment" -> {
                if(!out.contains("STAGE 9 COMPLETE: ALIGNED PASS | UNALIGNED PASS")) throw new IllegalStateException("Stage 9 failed");
                for(String mode:List.of("ALIGNED","UNALIGNED")) {
                    Matcher m=Pattern.compile("CHECKPOINT_DURABLE mode="+mode+" operator=(\\d+) channel=(\\d+) buffered=(\\d+)").matcher(out);
                    if(!m.find()) throw new IllegalStateException("Stage 9 checkpoint missing "+mode);
                    add(exp,mode,"operator_records",m.group(1),trial);
                    add(exp,mode,"channel_records",m.group(2),trial);
                    add(exp,mode,"buffered_events",m.group(3),trial);
                }
            }
            case "CheckpointFailureExperiment" -> {
                if(!out.contains("STAGE 12 COMPLETE: BASELINE PASS | MID-WRITE CRASH RECOVERY PASS")) throw new IllegalStateException("Stage 12 failed");
                add(exp,"CRASH_RECOVERY","replayed_events",extract(out,"RECOVERY PASS restored=10 replayed=(\\d+) final=30"),trial);
                add(exp,"CRASH_RECOVERY","partial_bytes",extract(out,"PARTIAL_WRITE checkpoint=20 bytes=(\\d+)"),trial);
            }
        }
    }
    static String run(String name,int trial) throws Exception {
        String classpath=System.getProperty("java.class.path");
        ProcessBuilder pb=new ProcessBuilder(javaExe(),"-cp",classpath,"edu.project.stream."+name);
        pb.redirectErrorStream(true);
        Path log=OUT.resolve(name+"-trial"+trial+".log");
        pb.redirectOutput(log.toFile());
        long start=System.nanoTime();
        Process p=pb.start();
        if(!p.waitFor(90,TimeUnit.SECONDS)) {p.destroyForcibly();p.waitFor();throw new IllegalStateException("Timed out: "+name);}
        double elapsed=(System.nanoTime()-start)/1_000_000.0;
        String output=Files.readString(log);
        if(p.exitValue()!=0) throw new IllegalStateException(name+" exited "+p.exitValue()+"; inspect "+log);
        parse(name,output,trial);
        runs.add(String.format(Locale.US,"%s,%d,PASS,%.3f,%s",name,trial,elapsed,csv(log.toString())));
        return String.format(Locale.US,"%.1f ms",elapsed);
    }
    static String svgEscape(String s){return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");}
    static void chart(String filename,String title,String experiment,String metric,String[] scenarios) throws IOException {
        Map<String,Double> means=new LinkedHashMap<>();
        for(String scenario:scenarios) {
            DoubleSummaryStatistics stat=samples.stream().filter(s->s.experiment.equals(experiment)&&s.metric.equals(metric)&&s.scenario.equals(scenario)).mapToDouble(Sample::value).summaryStatistics();
            if(stat.getCount()>0) means.put(scenario,stat.getAverage());
        }
        if(means.isEmpty())return;
        double max=means.values().stream().mapToDouble(Double::doubleValue).max().orElse(1);
        StringBuilder svg=new StringBuilder("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"900\" height=\""+(120+means.size()*75)+"\" viewBox=\"0 0 900 "+(120+means.size()*75)+"\"><rect width=\"100%\" height=\"100%\" fill=\"white\"/><text x=\"30\" y=\"38\" font-size=\"22\" font-family=\"Arial\" fill=\"#172b4d\">"+svgEscape(title)+"</text>");
        int i=0;
        for(var e:means.entrySet()) {
            int y=75+i++*75; double width=max==0?0:Math.max(0,e.getValue())/max*510;
            svg.append("<text x=\"30\" y=\"").append(y+20).append("\" font-family=\"Arial\" font-size=\"15\">"+svgEscape(e.getKey())+"</text>");
            svg.append("<rect x=\"240\" y=\"").append(y).append("\" width=\"").append(String.format(Locale.US,"%.2f",width)).append("\" height=\"32\" rx=\"5\" fill=\"#2476a8\"/>");
            svg.append("<text x=\"").append(String.format(Locale.US,"%.2f",255+width)).append("\" y=\"").append(y+22).append("\" font-family=\"Arial\" font-size=\"15\">"+String.format(Locale.US,"%.2f",e.getValue())+"</text>");
        }
        svg.append("<text x=\"30\" y=\"").append(110+means.size()*75).append("\" font-size=\"12\" font-family=\"Arial\" fill=\"#555\">Mean of successful trials; descriptive, not causal inference</text></svg>");
        Files.writeString(OUT.resolve(filename),svg.toString());
    }
    static void export() throws Exception {
        List<String> raw=new ArrayList<>(List.of("experiment,scenario,metric,value,trial"));
        for(Sample s:samples)raw.add(csv(s.experiment)+","+csv(s.scenario)+","+csv(s.metric)+","+String.format(Locale.US,"%.5f",s.value)+","+s.trial);
        Files.write(OUT.resolve("raw_metrics.csv"),raw);
        record Key(String exp,String scenario,String metric){}
        Map<Key,List<Double>> groups=new TreeMap<>(Comparator.comparing(Key::exp).thenComparing(Key::scenario).thenComparing(Key::metric));
        for(Sample s:samples)groups.computeIfAbsent(new Key(s.experiment,s.scenario,s.metric),k->new ArrayList<>()).add(s.value);
        List<String> summary=new ArrayList<>(List.of("experiment,scenario,metric,n,mean,stdev_sample,min,max"));
        for(var e:groups.entrySet()) {
            List<Double> v=e.getValue();double mean=v.stream().mapToDouble(x->x).average().orElse(0);
            double ss=0;for(double x:v)ss+=(x-mean)*(x-mean);
            double sd=v.size()>1?Math.sqrt(ss/(v.size()-1)):0;
            summary.add(csv(e.getKey().exp)+","+csv(e.getKey().scenario)+","+csv(e.getKey().metric)+","+v.size()+","+String.format(Locale.US,"%.5f,%.5f,%.5f,%.5f",mean,sd,Collections.min(v),Collections.max(v)));
        }
        Files.write(OUT.resolve("summary_statistics.csv"),summary);
        List<String> r=new ArrayList<>(List.of("experiment,trial,status,wall_clock_ms,log"));r.addAll(runs);Files.write(OUT.resolve("run_status.csv"),r);
        chart("stage6_throughput.svg","Stage 6: Throughput (events/s)","StragglerExperiment","throughput_events_sec",new String[]{"BASELINE","MODERATE","SEVERE","STALL"});
        chart("stage6_checkpoint.svg","Stage 6: Average checkpoint latency (ms)","StragglerExperiment","avg_checkpoint_ms",new String[]{"BASELINE","MODERATE","SEVERE","STALL"});
        chart("stage7_latency.svg","Stage 7: Average event latency (ms)","StragglerDetectionExperiment","avg_event_latency_ms",new String[]{"MONITOR_ONLY","ADAPTIVE_FLOW_CONTROL"});
        chart("stage7_throughput.svg","Stage 7: Throughput (events/s)","StragglerDetectionExperiment","throughput_events_sec",new String[]{"MONITOR_ONLY","ADAPTIVE_FLOW_CONTROL"});
        chart("stage8_barrier.svg","Stage 8: Barrier arrival span (ms)","BarrierAlignmentExperiment","barrier_span_ms",new String[]{"ALIGNED","UNALIGNED"});
        Files.writeString(OUT.resolve("README.txt"),"Stage 13 repeated-run benchmark. Each trial launches an independent JVM.\nRaw metrics and summary statistics are parsed from experiment stdout.\nSVG charts are descriptive means, not uncertainty plots.\nStage 6/7 metrics are wall-clock measured inside experiments; Stage 8 span is barrier arrival span, NOT unaligned snapshot initiation time.\nStage 9 and 12 validate crash recovery but do not measure storage fsync guarantees or power-loss durability.\nStage 10/11 are deterministic trace models and intentionally excluded from runtime-performance graphs.\nTrials are sequential on the same machine; JVM startup, scheduling and background load may influence measurements.\n");
    }
    public static void main(String[] args) throws Exception {
        int trials=args.length==0?5:Integer.parseInt(args[0]);
        if(trials<1||trials>30)throw new IllegalArgumentException("Trials must be 1..30");
        Files.createDirectories(OUT);
        System.out.println("STAGE 13: REPEATED EXPERIMENTS ("+trials+" trials each)");
        System.out.println("Requires Stage 6, 7, 8, 9, 12 classes compiled. Outputs: "+OUT.toAbsolutePath());
        try {
            for(int trial=1;trial<=trials;trial++) {
                for(String experiment:EXPERIMENTS) {
                    System.out.printf("Trial %d/%d %-39s ",trial,trials,experiment);
                    System.out.println(run(experiment,trial)+" PASS");
                }
            }
            export();
            System.out.println("STAGE 13 COMPLETE: "+runs.size()+" experiment executions PASS");
            System.out.println("CSV results, individual logs, and 5 SVG charts: "+OUT.toAbsolutePath());
        } catch(Exception ex) {
            Files.writeString(OUT.resolve("FAILED.txt"),ex.toString()+"\n");
            System.err.println("BENCHMARK FAILED: "+ex.getMessage());
            throw ex;
        }
    }
}
