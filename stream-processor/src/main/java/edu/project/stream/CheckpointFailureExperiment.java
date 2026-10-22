package edu.project.stream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

/** Stage 12: controlled real JVM crash while writing a checkpoint. */
public final class CheckpointFailureExperiment {
    private static final int TOTAL = 30;
    private static final int MAGIC = 0x43503132;
    private static final String COMMITTED = "committed.chk";
    private static final String TEMP = "pending.chk.tmp";

    private record State(int lastId, int count, long digest) {
        State next(int id) {
            if (id != lastId + 1) throw new IllegalStateException("Expected event " + (lastId + 1) + " got " + id);
            return new State(id, count + 1, digest * 31 + id);
        }
    }
    private static State initial() { return new State(0, 0, 7L); }
    private static byte[] encode(State s) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(out);
        d.writeInt(MAGIC); d.writeInt(s.lastId); d.writeInt(s.count); d.writeLong(s.digest); d.flush();
        byte[] data = out.toByteArray();
        CRC32 crc = new CRC32(); crc.update(data);
        ByteBuffer result = ByteBuffer.allocate(data.length + 8);
        result.put(data).putLong(crc.getValue());
        return result.array();
    }
    private static State decode(byte[] bytes) throws IOException {
        if (bytes.length != 28) throw new IOException("Invalid checkpoint length " + bytes.length);
        CRC32 crc = new CRC32(); crc.update(bytes, 0, 20);
        ByteBuffer b = ByteBuffer.wrap(bytes);
        if (b.getInt() != MAGIC) throw new IOException("Bad checkpoint magic");
        State s = new State(b.getInt(), b.getInt(), b.getLong());
        if (b.getLong() != crc.getValue() || s.lastId != s.count) throw new IOException("Checkpoint integrity failure");
        return s;
    }
    private static State restore(Path dir) throws IOException {
        Path p = dir.resolve(COMMITTED);
        return Files.exists(p) ? decode(Files.readAllBytes(p)) : initial();
    }
    private static void durableCheckpoint(Path dir, State s, boolean crash) throws IOException {
        byte[] bytes = encode(s);
        Path tmp = dir.resolve(TEMP);
        try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            if (crash) {
                channel.write(ByteBuffer.wrap(bytes, 0, 9));
                channel.force(true);
                System.out.println("PARTIAL_WRITE checkpoint=" + s.lastId + " bytes=9");
                System.out.flush();
                Runtime.getRuntime().halt(77); // real abrupt worker process exit, no cleanup
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        // The committed file is replaced only after the full temporary file is durable.
        Files.move(tmp, dir.resolve(COMMITTED), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
    private static void worker(Path dir) throws Exception {
        Files.createDirectories(dir);
        State state = restore(dir);
        System.out.println("READY restored=" + state.lastId);
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            String[] fields = line.split(" ");
            switch (fields[0]) {
                case "EVENT" -> { state = state.next(Integer.parseInt(fields[1])); System.out.println("ACK " + state.lastId); }
                case "CHECKPOINT" -> {
                    int expected = Integer.parseInt(fields[1]);
                    if (expected != state.lastId) throw new IllegalStateException("Checkpoint boundary mismatch");
                    durableCheckpoint(dir, state, fields.length > 2 && fields[2].equals("CRASH"));
                    System.out.println("COMMITTED " + state.lastId);
                }
                case "STATE" -> System.out.println("STATE " + state.lastId + " " + state.count + " " + state.digest);
                case "EXIT" -> { System.out.println("BYE"); return; }
                default -> throw new IllegalArgumentException(line);
            }
        }
    }
    private static final class Worker implements AutoCloseable {
        final Process process;
        final BufferedReader reader;
        final BufferedWriter writer;
        Worker(Path dir) throws Exception {
            String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            String cp = System.getProperty("java.class.path");
            process = new ProcessBuilder(java, "-cp", cp, CheckpointFailureExperiment.class.getName(), "--worker", dir.toString())
                .redirectErrorStream(true).start();
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
            String ready = reader.readLine();
            if (ready == null || !ready.startsWith("READY ")) throw new IOException("Worker failed to start: " + ready);
            System.out.println("  " + ready);
        }
        String command(String command) throws Exception {
            writer.write(command); writer.newLine(); writer.flush();
            String response = reader.readLine();
            if (response == null) throw new IOException("Worker terminated after " + command);
            return response;
        }
        void events(int first, int last) throws Exception {
            for (int i = first; i <= last; i++) {
                String ack = command("EVENT " + i);
                if (!ack.equals("ACK " + i)) throw new AssertionError(ack);
            }
        }
        void checkpoint(int id) throws Exception {
            String result = command("CHECKPOINT " + id + " NORMAL");
            if (!result.equals("COMMITTED " + id)) throw new AssertionError(result);
            System.out.println("  " + result);
        }
        State state() throws Exception {
            String[] parts = command("STATE").split(" ");
            if (parts.length != 4 || !parts[0].equals("STATE")) throw new AssertionError(Arrays.toString(parts));
            return new State(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), Long.parseLong(parts[3]));
        }
        @Override public void close() {
            if (process.isAlive()) {
                try { command("EXIT"); } catch (Exception ignored) { }
                process.destroy();
            }
            try { if (!process.waitFor(2, TimeUnit.SECONDS)) process.destroyForcibly(); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); process.destroyForcibly(); }
        }
    }
    private static void reset(Path dir) throws IOException {
        Files.createDirectories(dir);
        try (var paths = Files.list(dir)) {
            for (Path p : paths.toList()) Files.delete(p);
        }
    }
    private static State baseline(Path dir) throws Exception {
        System.out.println("\n===== BASELINE: THREE SUCCESSFUL CHECKPOINTS =====");
        reset(dir);
        try (Worker w = new Worker(dir)) {
            for (int checkpoint : new int[]{10, 20, 30}) {
                w.events(checkpoint - 9, checkpoint);
                w.checkpoint(checkpoint);
            }
            State s = w.state();
            if (!s.equals(restore(dir))) throw new AssertionError("Baseline committed state mismatch");
            System.out.println("  BASELINE PASS final=" + s.lastId);
            return s;
        }
    }
    private static State crashDuringCheckpoint(Path dir, State expected) throws Exception {
        System.out.println("\n===== FAILURE: JVM CRASH DURING CHECKPOINT 20 =====");
        reset(dir);
        byte[] committedBefore;
        try (Worker w = new Worker(dir)) {
            w.events(1, 10);
            w.checkpoint(10);
            committedBefore = Files.readAllBytes(dir.resolve(COMMITTED));
            w.events(11, 20);
            w.writer.write("CHECKPOINT 20 CRASH"); w.writer.newLine(); w.writer.flush();
            String partial = w.reader.readLine();
            if (partial == null || !partial.startsWith("PARTIAL_WRITE")) throw new AssertionError("Crash injection did not reach partial write");
            System.out.println("  " + partial);
            if (!w.process.waitFor(5, TimeUnit.SECONDS) || w.process.exitValue() != 77)
                throw new AssertionError("Worker did not crash with expected exit code 77");
            System.out.println("  WORKER JVM EXITED code=77");
        }
        if (!Arrays.equals(committedBefore, Files.readAllBytes(dir.resolve(COMMITTED))))
            throw new AssertionError("Committed checkpoint overwritten by partial write");
        if (!Files.exists(dir.resolve(TEMP)) || Files.size(dir.resolve(TEMP)) != 9)
            throw new AssertionError("Partial temporary checkpoint not found");
        System.out.println("  COMMITTED_CHECKPOINT_INTACT event=10 | partial temp bytes=9");
        try (Worker restarted = new Worker(dir)) {
            State restored = restarted.state();
            if (restored.lastId != 10) throw new AssertionError("Restored invalid checkpoint " + restored);
            restarted.events(11, TOTAL); // replay from last committed boundary
            restarted.checkpoint(TOTAL);
            State finalState = restarted.state();
            if (!finalState.equals(expected) || !finalState.equals(restore(dir)))
                throw new AssertionError("Recovered final state differs from baseline");
            System.out.println("  RECOVERY PASS restored=10 replayed=20 final=" + finalState.lastId);
            return finalState;
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("--worker")) { worker(Path.of(args[1])); return; }
        System.out.println("STAGE 12: ATOMIC DURABLE CHECKPOINT + MID-WRITE JVM CRASH");
        System.out.println("Separate Java worker processes; local disk; source replay is deterministic.");
        Path root = Path.of("stage12-checkpoints");
        State expected = baseline(root.resolve("baseline"));
        State recovered = crashDuringCheckpoint(root.resolve("crash"), expected);
        System.out.println("\n=============== COMPARISON ===============");
        System.out.printf("%-26s %-12s %-12s%n", "Metric", "Baseline", "Crash+Recovery");
        System.out.printf("%-26s %-12d %-12d%n", "Final events", expected.count, recovered.count);
        System.out.printf("%-26s %-12s %-12s%n", "Checkpoint 20", "COMMITTED", "REJECTED");
        System.out.printf("%-26s %-12d %-12d%n", "Replayed after restart", 0, 20);
        System.out.printf("%-26s %-12s %-12s%n", "Final digest", "MATCH", "MATCH");
        System.out.println("STAGE 12 COMPLETE: BASELINE PASS | MID-WRITE CRASH RECOVERY PASS");
    }
}
