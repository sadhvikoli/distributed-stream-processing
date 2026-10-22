package edu.project.stream;

import java.io.*;
import java.net.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Stage 6: Automated TCP straggler experiments using separate Worker JVMs. */
public class StragglerExperiment {
    static final int EVENTS = 40;
    static final int CHECKPOINT_EVERY = 10;
    static final int SOURCE_INTERVAL_MS = 8;
    static final int FIRST_SLOW_EVENT = 11;
    static final int LAST_SLOW_EVENT = 25;
    static final int BASE_PORT = 19100;

    enum Scenario {
        BASELINE(0, 0), MODERATE(25, 0), SEVERE(70, 0), STALL(0, 400);
        final int delayMs;
        final int stallMs;
        Scenario(int delayMs, int stallMs) {
            this.delayMs = delayMs;
            this.stallMs = stallMs;
        }
    }

    static void require(String actual, String expected) throws IOException {
        if (!expected.equals(actual)) {
            throw new IOException("Expected '" + expected + "' but received '" + actual + "'");
        }
    }

    static Socket connect(int port) throws Exception {
        IOException last = null;
        for (int i = 0; i < 120; i++) {
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress("127.0.0.1", port), 300);
                socket.setSoTimeout(20000);
                return socket;
            } catch (IOException e) {
                last = e;
                Thread.sleep(40);
            }
        }
        throw new IOException("Could not connect to port " + port, last);
    }

    static void worker(int index, int portBase, Scenario scenario) throws Exception {
        try (ServerSocket server = new ServerSocket(portBase + index)) {
            server.setReuseAddress(true);
            try (Socket upstream = server.accept();
                 BufferedReader in = new BufferedReader(new InputStreamReader(upstream.getInputStream()));
                 PrintWriter upstreamAck = new PrintWriter(upstream.getOutputStream(), true)) {
                Socket downstream = null;
                try {
                    if (index < 3) downstream = connect(portBase + index + 1);
                    PrintWriter forward = downstream == null ? null :
                        new PrintWriter(downstream.getOutputStream(), true);
                    BufferedReader downstreamAck = downstream == null ? null :
                        new BufferedReader(new InputStreamReader(downstream.getInputStream()));
                    int lastEvent = 0;
                    String line;
                    while ((line = in.readLine()) != null) {
                        if (line.startsWith("EVENT ")) {
                            int id = Integer.parseInt(line.substring(6));
                            if (id != lastEvent + 1) {
                                throw new IOException("Worker-" + index + " unexpected event " + id);
                            }
                            if (index == 2) {
                                if (scenario == Scenario.STALL && id == 18) {
                                    Thread.sleep(scenario.stallMs);
                                } else if (id >= FIRST_SLOW_EVENT && id <= LAST_SLOW_EVENT) {
                                    Thread.sleep(scenario.delayMs);
                                }
                            }
                            lastEvent = id;
                            if (forward != null) {
                                forward.println(line);
                                if (forward.checkError()) throw new IOException("Downstream send failed");
                                require(downstreamAck.readLine(), "EVENT_OK " + id);
                            }
                            upstreamAck.println("EVENT_OK " + id);
                        } else if (line.startsWith("CHECKPOINT ")) {
                            int id = Integer.parseInt(line.substring(11));
                            if (forward != null) {
                                forward.println(line);
                                if (forward.checkError()) throw new IOException("Downstream send failed");
                                require(downstreamAck.readLine(), "CHECKPOINT_OK " + id);
                            }
                            upstreamAck.println("CHECKPOINT_OK " + id);
                        } else if (line.equals("END")) {
                            if (forward != null) {
                                forward.println("END");
                                require(downstreamAck.readLine(), "END_OK");
                            }
                            upstreamAck.println("END_OK");
                            break;
                        } else {
                            throw new IOException("Unknown message: " + line);
                        }
                    }
                } finally {
                    if (downstream != null) downstream.close();
                }
            }
        }
    }

    static class Result {
        Scenario scenario;
        double elapsedMs;
        double throughput;
        double averageCheckpointMs;
        double maxCheckpointMs;
        double checkpoint20Ms;
        double checkpoint30Ms;
        int peakOutstanding;
        boolean pass;
    }

    static Result runScenario(Scenario scenario, int number) throws Exception {
        int portBase = BASE_PORT + number * 10;
        String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        String cp = System.getProperty("java.class.path");
        List<Process> processes = new ArrayList<>();
        ExecutorService writerPool = Executors.newSingleThreadExecutor();
        System.out.println("\n===== " + scenario + " =====");
        try {
            for (int i = 3; i >= 1; i--) {
                Process p = new ProcessBuilder(java, "-cp", cp,
                    StragglerExperiment.class.getName(), "worker", String.valueOf(i),
                    String.valueOf(portBase), scenario.name()).inheritIO().start();
                processes.add(p);
            }
            try (Socket source = connect(portBase + 1);
                 BufferedReader ack = new BufferedReader(new InputStreamReader(source.getInputStream()));
                 PrintWriter send = new PrintWriter(source.getOutputStream(), true)) {

                ConcurrentMap<Integer, Long> checkpointSent = new ConcurrentHashMap<>();
                ConcurrentMap<Integer, Double> checkpointLatencies = new ConcurrentHashMap<>();
                AtomicInteger sent = new AtomicInteger();
                AtomicInteger received = new AtomicInteger();
                AtomicInteger peakOutstanding = new AtomicInteger();
                long start = System.nanoTime();

                Future<?> sending = writerPool.submit(() -> {
                    try {
                        for (int id = 1; id <= EVENTS; id++) {
                            send.println("EVENT " + id);
                            if (send.checkError()) throw new IOException("Source send failed");
                            int inFlight = sent.incrementAndGet() - received.get();
                            peakOutstanding.accumulateAndGet(inFlight, Math::max);
                            if (id % CHECKPOINT_EVERY == 0) {
                                int checkpoint = id / CHECKPOINT_EVERY;
                                checkpointSent.put(checkpoint, System.nanoTime());
                                send.println("CHECKPOINT " + checkpoint);
                                if (send.checkError()) throw new IOException("Barrier send failed");
                            }
                            Thread.sleep(SOURCE_INTERVAL_MS);
                        }
                        send.println("END");
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });

                int eventAcks = 0;
                int checkpointAcks = 0;
                while (true) {
                    String line = ack.readLine();
                    if (line == null) throw new IOException("Worker pipeline closed unexpectedly");
                    if (line.startsWith("EVENT_OK ")) {
                        int id = Integer.parseInt(line.substring(9));
                        if (id != eventAcks + 1) throw new IOException("Out-of-order ACK: " + line);
                        eventAcks++;
                        received.incrementAndGet();
                    } else if (line.startsWith("CHECKPOINT_OK ")) {
                        int id = Integer.parseInt(line.substring(14));
                        if (id != checkpointAcks + 1) throw new IOException("Out-of-order checkpoint: " + line);
                        checkpointAcks++;
                        Long started = checkpointSent.get(id);
                        if (started == null) throw new IOException("Missing checkpoint start " + id);
                        double ms = (System.nanoTime() - started) / 1_000_000.0;
                        checkpointLatencies.put(id, ms);
                        System.out.printf(Locale.US, "Checkpoint %d latency: %.2f ms%n", id, ms);
                    } else if (line.equals("END_OK")) {
                        break;
                    } else {
                        throw new IOException("Unexpected ACK: " + line);
                    }
                }
                sending.get(25, TimeUnit.SECONDS);
                long finish = System.nanoTime();
                Result r = new Result();
                r.scenario = scenario;
                r.elapsedMs = (finish - start) / 1_000_000.0;
                r.throughput = EVENTS * 1000.0 / r.elapsedMs;
                r.averageCheckpointMs = checkpointLatencies.values().stream()
                    .mapToDouble(Double::doubleValue).average().orElse(0);
                r.maxCheckpointMs = checkpointLatencies.values().stream()
                    .mapToDouble(Double::doubleValue).max().orElse(0);
                r.checkpoint20Ms = checkpointLatencies.getOrDefault(2, 0.0);
                r.checkpoint30Ms = checkpointLatencies.getOrDefault(3, 0.0);
                r.peakOutstanding = peakOutstanding.get();
                r.pass = eventAcks == EVENTS && checkpointAcks == EVENTS / CHECKPOINT_EVERY;
                if (!r.pass) throw new IllegalStateException("Incorrect ACK counts");
                return r;
            }
        } finally {
            writerPool.shutdownNow();
            for (Process p : processes) {
                if (p.isAlive()) p.destroy();
            }
            for (Process p : processes) {
                if (!p.waitFor(2, TimeUnit.SECONDS)) {
                    p.destroyForcibly();
                    p.waitFor(2, TimeUnit.SECONDS);
                }
            }
        }
    }

    static void coordinator() throws Exception {
        System.out.println("STAGE 6: AUTOMATED TCP STRAGGLER EXPERIMENTS");
        System.out.println("40 events; source interval 8 ms; checkpoint every 10 events");
        System.out.println("Worker-2 slowdown applies to events 11-25; stall occurs at event 18");
        List<Result> results = new ArrayList<>();
        int i = 0;
        for (Scenario scenario : Scenario.values()) results.add(runScenario(scenario, i++));
        System.out.println("\n================ EXPERIMENT COMPARISON ================");
        System.out.printf("%-12s %12s %12s %12s %12s %12s %8s%n",
            "Scenario", "Elapsed ms", "Events/sec", "Avg CP ms", "CP20 ms", "CP30 ms", "PASS");
        for (Result r : results) {
            System.out.printf(Locale.US, "%-12s %12.2f %12.2f %12.2f %12.2f %12.2f %8s%n",
                r.scenario, r.elapsedMs, r.throughput, r.averageCheckpointMs,
                r.checkpoint20Ms, r.checkpoint30Ms, r.pass ? "PASS" : "FAIL");
        }
        System.out.println("Checkpoint latency is measured from source barrier send to pipeline ACK.");
        System.out.println("CP20 and CP30 correspond to barriers after events 20 and 30.");
        System.out.println("All times are real elapsed wall-clock measurements, not modeled costs.");
        System.out.println("Outstanding events are an application-level proxy, not TCP buffer occupancy.");
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("worker")) {
            worker(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Scenario.valueOf(args[3]));
        } else {
            coordinator();
        }
    }
}
