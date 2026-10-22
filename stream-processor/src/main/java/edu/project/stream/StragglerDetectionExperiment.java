package edu.project.stream;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Standalone Stage 7: real TCP pipeline, monitoring, adaptive flow control. */
public class StragglerDetectionExperiment {
    static final int N = 60, PORT = 19610, BASE_DELAY_MS = 8;
    static final int SLOW_FROM = 11, SLOW_TO = 35, SLOW_MS = 45;
    static final long DETECT_NS = TimeUnit.MILLISECONDS.toNanos(90);
    static final String[] FRUITS = {"apple", "banana", "apple", "orange", "grape"};

    static void worker(int index) throws Exception {
        try (ServerSocket server = new ServerSocket(PORT + index);
             Socket upstream = server.accept()) {
            upstream.setTcpNoDelay(true);
            BufferedReader input = new BufferedReader(new InputStreamReader(upstream.getInputStream()));
            PrintWriter output = new PrintWriter(upstream.getOutputStream(), true);
            Socket downstream = null;
            try {
                if (index < 3) {
                    downstream = connect(PORT + index + 1);
                    downstream.setTcpNoDelay(true);
                }
                PrintWriter nextOut = downstream == null ? null : new PrintWriter(downstream.getOutputStream(), true);
                BufferedReader nextIn = downstream == null ? null : new BufferedReader(new InputStreamReader(downstream.getInputStream()));
                int last = 0;
                Map<String, Integer> counts = new TreeMap<>();
                String line;
                while ((line = input.readLine()) != null) {
                    if (line.equals("END")) {
                        if (nextOut != null) { nextOut.println(line); expect(nextIn, "END_OK"); }
                        output.println("END_OK");
                        break;
                    }
                    String[] p = line.split(" ");
                    if (p.length != 3 || !p[0].equals("EVENT")) throw new IOException("Bad event: " + line);
                    int id = Integer.parseInt(p[1]);
                    if (id != last + 1) throw new IOException("Worker-" + index + " expected " + (last + 1) + " got " + id);
                    if (index == 2 && id >= SLOW_FROM && id <= SLOW_TO) Thread.sleep(SLOW_MS);
                    counts.merge(p[2], 1, Integer::sum);
                    last = id;
                    if (nextOut != null) { nextOut.println(line); expect(nextIn, "OK " + id); }
                    output.println("OK " + id);
                }
                Map<String, Integer> expected = new TreeMap<>();
                for (int id = 1; id <= N; id++) expected.merge(FRUITS[(id - 1) % FRUITS.length], 1, Integer::sum);
                if (last != N || !counts.equals(expected)) throw new IOException("Worker-" + index + " incorrect state");
                System.out.println("Worker-" + index + " verified " + last + " events: PASS");
            } finally { if (downstream != null) downstream.close(); }
        }
    }

    static void expect(BufferedReader r, String expected) throws IOException {
        String actual = r.readLine();
        if (!expected.equals(actual)) throw new IOException("Expected " + expected + " got " + actual);
    }

    static Socket connect(int port) throws Exception {
        for (int i = 0; i < 120; i++) {
            try { return new Socket("127.0.0.1", port); }
            catch (ConnectException ex) { Thread.sleep(25); }
        }
        throw new IOException("Cannot connect to " + port);
    }

    static class Result {
        String mode;
        double elapsedMs, throughput, avgLatencyMs, maxLatencyMs;
        int detections, peakOutstanding, flowControlActivations;
        boolean correct;
    }

    static Result run(String mode, boolean adaptive) throws Exception {
        System.out.println("\n========== " + mode + " ==========");
        List<Process> children = new ArrayList<>();
        String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        String cp = System.getProperty("java.class.path");
        for (int index = 3; index >= 1; index--) {
            children.add(new ProcessBuilder(java, "-cp", cp,
                StragglerDetectionExperiment.class.getName(), "worker", String.valueOf(index))
                .inheritIO().start());
        }
        Socket source = null;
        ExecutorService readerThread = Executors.newSingleThreadExecutor();
        try {
            source = connect(PORT + 1);
            source.setTcpNoDelay(true);
            source.setSoTimeout(20000);
            PrintWriter send = new PrintWriter(source.getOutputStream(), true);
            BufferedReader receive = new BufferedReader(new InputStreamReader(source.getInputStream()));
            final Object lock = new Object();
            final long[] sentAt = new long[N + 1];
            final long[] latency = new long[N + 1];
            final boolean[] warned = new boolean[N + 1];
            final int[] acked = {0};
            final Throwable[] error = {null};
            final int[] detections = {0};
            final int[] maxOutstanding = {0};
            final int[] activations = {0};
            final boolean[] throttled = {false};
            long started = System.nanoTime();

            Future<?> reader = readerThread.submit(() -> {
                try {
                    for (int id = 1; id <= N; id++) {
                        expect(receive, "OK " + id);
                        synchronized (lock) {
                            latency[id] = System.nanoTime() - sentAt[id];
                            acked[0] = id;
                            lock.notifyAll();
                        }
                    }
                } catch (Throwable ex) {
                    synchronized (lock) { error[0] = ex; lock.notifyAll(); }
                }
            });

            for (int id = 1; id <= N; id++) {
                synchronized (lock) {
                    // A small window when throttled; a larger one otherwise.
                    while (id - acked[0] > (adaptive && throttled[0] ? 3 : 32)) {
                        if (error[0] != null) throw new IOException("ACK reader failed", error[0]);
                        inspectOldest(lock, sentAt, warned, acked, id - 1, detections, adaptive, throttled, activations);
                        lock.wait(5);
                    }
                    inspectOldest(lock, sentAt, warned, acked, id - 1, detections, adaptive, throttled, activations);
                    sentAt[id] = System.nanoTime();
                    send.println("EVENT " + id + " " + FRUITS[(id - 1) % FRUITS.length]);
                    maxOutstanding[0] = Math.max(maxOutstanding[0], id - acked[0]);
                    // Leave throttled mode only when backlog has drained and the last
                    // completed event was recent enough; this is flow control, not acceleration.
                    if (adaptive && throttled[0] && id > SLOW_TO + 4 && id - acked[0] <= 2 && acked[0] > 0
                        && latency[acked[0]] < TimeUnit.MILLISECONDS.toNanos(35)) {
                        throttled[0] = false;
                        System.out.println("Backlog drained: restored normal send window");
                    }
                }
                Thread.sleep(BASE_DELAY_MS);
            }
            synchronized (lock) {
                while (acked[0] < N) {
                    if (error[0] != null) throw new IOException("ACK reader failed", error[0]);
                    inspectOldest(lock, sentAt, warned, acked, N, detections, adaptive, throttled, activations);
                    lock.wait(5);
                }
            }
            send.println("END");
            expect(receive, "END_OK");
            reader.get(3, TimeUnit.SECONDS);
            boolean correct = true;
            for (Process p : children) {
                if (!p.waitFor(4, TimeUnit.SECONDS) || p.exitValue() != 0) correct = false;
            }
            long elapsed = System.nanoTime() - started;
            double sum = 0, max = 0;
            for (int id = 1; id <= N; id++) {
                sum += latency[id] / 1_000_000.0;
                max = Math.max(max, latency[id] / 1_000_000.0);
            }
            Result r = new Result();
            r.mode = mode;
            r.elapsedMs = elapsed / 1_000_000.0;
            r.throughput = N * 1_000_000_000.0 / elapsed;
            r.avgLatencyMs = sum / N;
            r.maxLatencyMs = max;
            r.detections = detections[0];
            r.peakOutstanding = maxOutstanding[0];
            r.flowControlActivations = activations[0];
            r.correct = correct;
            System.out.printf(Locale.US, "Elapsed %.1f ms | throughput %.1f/s | avg event latency %.1f ms | peak outstanding %d | PASS=%s%n",
                r.elapsedMs, r.throughput, r.avgLatencyMs, r.peakOutstanding, correct);
            if (!correct) throw new IllegalStateException("Worker verification failed");
            return r;
        } finally {
            if (source != null) source.close();
            readerThread.shutdownNow();
            for (Process p : children) if (p.isAlive()) p.destroyForcibly();
            for (Process p : children) p.waitFor(2, TimeUnit.SECONDS);
        }
    }

    static void inspectOldest(Object lock, long[] sentAt, boolean[] warned, int[] acked, int sent,
                              int[] detections, boolean adaptive, boolean[] throttled, int[] activations) {
        int oldest = acked[0] + 1;
        if (oldest > sent || sentAt[oldest] == 0) return;
        if (!warned[oldest] && System.nanoTime() - sentAt[oldest] >= DETECT_NS) {
            warned[oldest] = true;
            detections[0]++;
            System.out.println("STRAGGLER SUSPECTED: event " + oldest + " unacknowledged for >90 ms (not declared crashed)");
            if (adaptive && !throttled[0]) {
                throttled[0] = true;
                activations[0]++;
                System.out.println("MITIGATION: reduce source in-flight window to 3 events");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("worker")) {
            worker(Integer.parseInt(args[1]));
            return;
        }
        System.out.println("STAGE 7: TCP STRAGGLER DETECTION AND FLOW CONTROL");
        System.out.println("60 events; Worker-2 delayed 45 ms/event during events 11-35; detection threshold 90 ms");
        Result a = run("MONITOR_ONLY", false);
        Result b = run("ADAPTIVE_FLOW_CONTROL", true);
        System.out.println("\n================ COMPARISON ================");
        System.out.printf("%-23s %12s %12s%n", "Metric", "Monitor", "Adaptive");
        System.out.printf(Locale.US, "%-23s %12.2f %12.2f%n", "Elapsed (ms)", a.elapsedMs, b.elapsedMs);
        System.out.printf(Locale.US, "%-23s %12.2f %12.2f%n", "Throughput (events/s)", a.throughput, b.throughput);
        System.out.printf(Locale.US, "%-23s %12.2f %12.2f%n", "Avg event latency (ms)", a.avgLatencyMs, b.avgLatencyMs);
        System.out.printf(Locale.US, "%-23s %12.2f %12.2f%n", "Max event latency (ms)", a.maxLatencyMs, b.maxLatencyMs);
        System.out.printf("%-23s %12d %12d%n", "Peak outstanding events", a.peakOutstanding, b.peakOutstanding);
        System.out.printf("%-23s %12d %12d%n", "Straggler warnings", a.detections, b.detections);
        System.out.printf("%-23s %12d %12d%n", "Flow control activations", a.flowControlActivations, b.flowControlActivations);
        System.out.printf("%-23s %12s %12s%n", "Correctness", a.correct ? "PASS" : "FAIL", b.correct ? "PASS" : "FAIL");
        System.out.println("Detection uses end-to-end event ACK delay, not per-worker timing; Worker-2 is known from injected fault.");
        System.out.println("Outstanding events are application-level counts, not measured TCP buffer occupancy.");
    }
}
