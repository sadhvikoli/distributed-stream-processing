package edu.project.stream;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Stage 8: two real TCP input channels into a merge operator.
 * Demonstrates aligned barriers versus unaligned channel-state capture.
 * One checkpoint per run, after 10 records on each channel.
 */
public class BarrierAlignmentExperiment {
    static final int BEFORE = 10, AFTER = 10;
    static final int SLOW_BARRIER_MS = 300;
    static final String[] CHANNELS = {"A", "B"};

    enum Mode { ALIGNED, UNALIGNED }

    static class Message {
        final String channel;
        final String type;
        final int id;
        Message(String channel, String type, int id) {
            this.channel = channel; this.type = type; this.id = id;
        }
    }

    static class Outcome {
        Mode mode;
        double checkpointMs, elapsedMs;
        int buffered, channelRecords, processed;
        boolean pass;
    }

    static class Merge {
        final Mode mode;
        final BlockingQueue<Message> incoming = new LinkedBlockingQueue<>();
        final Set<String> barriers = new HashSet<>();
        final Map<String, Set<Integer>> state = emptyState();
        final Map<String, Set<Integer>> channelState = emptyState();
        final List<Message> buffered = new ArrayList<>();
        Map<String, Set<Integer>> snapshot;
        long firstBarrierNs, completedNs;
        int maxBuffered, done;
        boolean checkpointCompleted;

        Merge(Mode mode) { this.mode = mode; }

        void process(Message m) {
            if (!state.get(m.channel).add(m.id)) {
                throw new IllegalStateException("Duplicate " + m.channel + m.id);
            }
        }

        void handle(Message m) {
            if (m.type.equals("END")) { done++; return; }
            if (m.type.equals("BARRIER")) {
                if (!barriers.add(m.channel)) {
                    throw new IllegalStateException("Duplicate barrier: " + m.channel);
                }
                if (barriers.size() == 1) {
                    firstBarrierNs = System.nanoTime();
                    if (mode == Mode.UNALIGNED) {
                        // Capture operator state at first barrier. Events arriving
                        // from the other channel until its barrier are channel state.
                        snapshot = copyState(state);
                    }
                }
                if (barriers.size() == 2) {
                    if (mode == Mode.ALIGNED) {
                        snapshot = copyState(state);
                    }
                    completedNs = System.nanoTime();
                    checkpointCompleted = true;
                    // Aligned: release buffered post-barrier records only after snapshot.
                    if (mode == Mode.ALIGNED) {
                        for (Message pending : buffered) process(pending);
                        buffered.clear();
                    }
                }
                return;
            }
            if (!m.type.equals("EVENT")) throw new IllegalStateException(m.type);
            if (mode == Mode.ALIGNED && !checkpointCompleted
                    && barriers.contains(m.channel)) {
                buffered.add(m);
                maxBuffered = Math.max(maxBuffered, buffered.size());
                return;
            }
            if (mode == Mode.UNALIGNED && !checkpointCompleted
                    && barriers.size() == 1 && !barriers.contains(m.channel)) {
                // These events were in flight on the channel that had not yet
                // delivered its barrier when the operator state was captured.
                channelState.get(m.channel).add(m.id);
            }
            process(m);
        }
    }

    static Map<String, Set<Integer>> emptyState() {
        Map<String, Set<Integer>> result = new LinkedHashMap<>();
        for (String channel : CHANNELS) result.put(channel, new HashSet<>());
        return result;
    }

    static Map<String, Set<Integer>> copyState(Map<String, Set<Integer>> source) {
        Map<String, Set<Integer>> result = emptyState();
        for (String c : CHANNELS) result.get(c).addAll(source.get(c));
        return result;
    }

    static void sender(int port, String channel, CountDownLatch start,
                       BlockingQueue<Throwable> errors) {
        try (Socket socket = new Socket("127.0.0.1", port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            socket.setTcpNoDelay(true);
            out.println("HELLO " + channel);
            start.await();
            for (int id = 1; id <= BEFORE; id++) {
                out.println("EVENT " + id);
                Thread.sleep(3);
                if (channel.equals("B") && id == 5) Thread.sleep(SLOW_BARRIER_MS);
            }
            out.println("BARRIER 1");
            for (int id = BEFORE + 1; id <= BEFORE + AFTER; id++) {
                out.println("EVENT " + id);
                Thread.sleep(3);
            }
            out.println("END");
            if (out.checkError()) throw new IOException("Sender disconnected: " + channel);
        } catch (Throwable e) { errors.add(e); }
    }

    static void readChannel(Socket socket, Merge merge,
                            BlockingQueue<Throwable> errors) {
        try (socket; BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream()))) {
            String hello = reader.readLine();
            if (hello == null || !hello.matches("HELLO [AB]")) {
                throw new IOException("Invalid handshake: " + hello);
            }
            String channel = hello.substring(6);
            String line;
            boolean ended = false;
            while ((line = reader.readLine()) != null) {
                if (line.equals("END")) {
                    merge.incoming.put(new Message(channel, "END", 0));
                    ended = true;
                    break;
                }
                String[] parts = line.split(" ");
                if (parts.length != 2 || !(parts[0].equals("EVENT") || parts[0].equals("BARRIER"))) {
                    throw new IOException("Bad message: " + line);
                }
                merge.incoming.put(new Message(channel, parts[0], Integer.parseInt(parts[1])));
            }
            if (!ended) throw new EOFException("Channel " + channel + " ended early");
        } catch (Throwable e) { errors.add(e); }
    }

    static void check(boolean ok, String message) {
        if (!ok) throw new IllegalStateException(message);
    }

    static Outcome run(Mode mode) throws Exception {
        Merge merge = new Merge(mode);
        BlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        long startNs = System.nanoTime();
        try (ServerSocket server = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            int port = server.getLocalPort();
            server.setSoTimeout(5000);
            pool.submit(() -> sender(port, "A", start, errors));
            pool.submit(() -> sender(port, "B", start, errors));
            Socket one = server.accept();
            Socket two = server.accept();
            pool.submit(() -> readChannel(one, merge, errors));
            pool.submit(() -> readChannel(two, merge, errors));
            start.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
            while (merge.done < 2 && System.nanoTime() < deadline) {
                Throwable error = errors.peek();
                if (error != null) throw new IllegalStateException("TCP thread failed", error);
                Message m = merge.incoming.poll(100, TimeUnit.MILLISECONDS);
                if (m != null) merge.handle(m);
            }
            check(merge.done == 2, "Timed out waiting for both inputs");
            check(errors.isEmpty(), "TCP thread error: " + errors.peek());
            check(merge.checkpointCompleted, "Checkpoint did not complete");
            check(merge.snapshot != null, "Missing snapshot");

            // Verify full stream and checkpoint cut: events 1..10 from each input.
            for (String channel : CHANNELS) {
                Set<Integer> expectedAll = new HashSet<>();
                Set<Integer> expectedCheckpoint = new HashSet<>();
                for (int id = 1; id <= BEFORE + AFTER; id++) expectedAll.add(id);
                for (int id = 1; id <= BEFORE; id++) expectedCheckpoint.add(id);
                check(merge.state.get(channel).equals(expectedAll),
                      "Final state incorrect on " + channel);
                Set<Integer> restored = new HashSet<>(merge.snapshot.get(channel));
                restored.addAll(merge.channelState.get(channel));
                check(restored.equals(expectedCheckpoint),
                      "Checkpoint cut incorrect on " + channel + ": " + restored);
            }
            Outcome outcome = new Outcome();
            outcome.mode = mode;
            outcome.checkpointMs = (merge.completedNs - merge.firstBarrierNs) / 1_000_000.0;
            outcome.elapsedMs = (System.nanoTime() - startNs) / 1_000_000.0;
            outcome.buffered = merge.maxBuffered;
            outcome.channelRecords = merge.channelState.values().stream().mapToInt(Set::size).sum();
            outcome.processed = merge.state.values().stream().mapToInt(Set::size).sum();
            outcome.pass = true;
            System.out.printf(Locale.US,
                    "%s: checkpoint barrier span %.2f ms | buffered %d | channel-state records %d | processed %d | PASS%n",
                    mode, outcome.checkpointMs, outcome.buffered,
                    outcome.channelRecords, outcome.processed);
            return outcome;
        } finally {
            start.countDown();
            pool.shutdownNow();
            pool.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("STAGE 8: TWO-INPUT TCP BARRIER ALIGNMENT");
        System.out.println("Two source sockets, 20 events each; Source B stalls after event 5 for "
                + SLOW_BARRIER_MS + " ms");
        Outcome aligned = run(Mode.ALIGNED);
        Outcome unaligned = run(Mode.UNALIGNED);
        System.out.println("\n============== COMPARISON ==============");
        System.out.printf("%-27s %12s %12s%n", "Metric", "ALIGNED", "UNALIGNED");
        System.out.printf(Locale.US, "%-27s %12.2f %12.2f%n", "Barrier span (ms)",
                aligned.checkpointMs, unaligned.checkpointMs);
        System.out.printf("%-27s %12d %12d%n", "Max buffered events", aligned.buffered, unaligned.buffered);
        System.out.printf("%-27s %12d %12d%n", "In-flight records captured", aligned.channelRecords, unaligned.channelRecords);
        System.out.printf("%-27s %12d %12d%n", "Events processed", aligned.processed, unaligned.processed);
        System.out.printf("%-27s %12s %12s%n", "Correctness", "PASS", "PASS");
        System.out.println("Barrier span is first-barrier to both-barriers arrival; it is NOT unaligned snapshot initiation latency.");
        System.out.println("Snapshots are in memory; this does not benchmark durable storage or crash recovery.");
    }
}
