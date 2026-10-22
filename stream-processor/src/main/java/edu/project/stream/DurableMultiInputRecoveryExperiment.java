package edu.project.stream;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Stage 9: real child JVM termination, durable aligned/unaligned checkpoint, TCP recovery. */
public class DurableMultiInputRecoveryExperiment {
    static final int PORT_A = 19721, PORT_B = 19722;
    static final int BEFORE = 10, TOTAL = 20;
    static final Path DIR = Paths.get("stage9-checkpoints");
    enum Mode { ALIGNED, UNALIGNED }
    record Msg(String channel, String kind, int id) {}
    static class State {
        final Map<String, Set<Integer>> applied = new LinkedHashMap<>();
        State() { applied.put("A", new HashSet<>()); applied.put("B", new HashSet<>()); }
        void add(String c, int id) {
            if (!applied.get(c).add(id)) throw new IllegalStateException("Duplicate " + c + ":" + id);
        }
        int count() { return applied.get("A").size() + applied.get("B").size(); }
    }
    static Path file(Mode mode) { return DIR.resolve(mode.name().toLowerCase(Locale.ROOT) + ".bin"); }
    static void require(boolean b, String message) { if (!b) throw new IllegalStateException(message); }

    // Durable format: mode, operator state, and in-flight channel records. Atomic rename after fsync.
    static void save(Mode mode, State state, Map<String, Set<Integer>> channels) throws Exception {
        Files.createDirectories(DIR);
        Path tmp = DIR.resolve(mode.name().toLowerCase(Locale.ROOT) + ".tmp");
        try (FileOutputStream stream = new FileOutputStream(tmp.toFile());
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(stream))) {
            out.writeUTF("STAGE9_V1"); out.writeUTF(mode.name());
            for (String c : List.of("A", "B")) {
                for (Set<Integer> ids : List.of(state.applied.get(c), channels.get(c))) {
                    out.writeInt(ids.size());
                    for (int id : new TreeSet<>(ids)) out.writeInt(id);
                }
            }
            out.flush();
            stream.getFD().sync();
        }
        Files.move(tmp, file(mode), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
    static State restore(Mode mode) throws Exception {
        State state = new State();
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file(mode))))) {
            require(in.readUTF().equals("STAGE9_V1"), "Wrong checkpoint format");
            require(in.readUTF().equals(mode.name()), "Checkpoint mode mismatch");
            for (String c : List.of("A", "B")) {
                Set<Integer> operator = readIds(in);
                Set<Integer> channel = readIds(in);
                for (int id : operator) state.add(c, id);
                for (int id : channel) state.add(c, id); // replay in-flight messages exactly once
            }
            require(in.read() == -1, "Trailing checkpoint bytes");
        }
        for (String c : List.of("A", "B")) {
            for (int i = 1; i <= BEFORE; i++) require(state.applied.get(c).contains(i), "Missing checkpoint event " + c + i);
            require(state.applied.get(c).size() == BEFORE, "Unexpected checkpoint boundary on " + c);
        }
        return state;
    }
    static Set<Integer> readIds(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > TOTAL) throw new IOException("Invalid set length " + n);
        Set<Integer> ids = new HashSet<>();
        for (int i = 0; i < n; i++) if (!ids.add(in.readInt())) throw new IOException("Duplicate stored event");
        return ids;
    }
    static class Worker {
        final Mode mode;
        final BlockingQueue<Msg> queue = new LinkedBlockingQueue<>();
        final BlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
        final State state;
        final Map<String, Set<Integer>> channelState = new LinkedHashMap<>();
        final Set<String> barriers = new HashSet<>();
        final List<Msg> buffered = new ArrayList<>();
        int maxBuffered;
        State snapshot;
        boolean complete;
        Worker(Mode mode, boolean recovering) throws Exception {
            this.mode = mode;
            this.state = recovering ? restore(mode) : new State();
            channelState.put("A", new HashSet<>()); channelState.put("B", new HashSet<>());
        }
        void handle(Msg msg) throws Exception {
            if (msg.kind.equals("END")) return;
            if (msg.kind.equals("BARRIER")) {
                require(barriers.add(msg.channel), "Duplicate barrier");
                if (barriers.size() == 1 && mode == Mode.UNALIGNED) {
                    snapshot = copy(state);
                }
                if (barriers.size() == 2) {
                    if (mode == Mode.ALIGNED) snapshot = copy(state);
                    save(mode, snapshot, channelState);
                    complete = true;
                    System.out.printf(Locale.US, "CHECKPOINT_DURABLE mode=%s operator=%d channel=%d buffered=%d%n",
                        mode, snapshot.count(), channelState.get("A").size() + channelState.get("B").size(), maxBuffered);
                    System.out.flush();
                    for (Msg m : buffered) state.add(m.channel, m.id);
                    buffered.clear();
                }
                return;
            }
            require(msg.kind.equals("EVENT"), "Bad kind");
            if (!complete && mode == Mode.ALIGNED && barriers.contains(msg.channel)) {
                buffered.add(msg); maxBuffered = Math.max(maxBuffered, buffered.size()); return;
            }
            if (!complete && mode == Mode.UNALIGNED && barriers.size() == 1 && !barriers.contains(msg.channel)) {
                channelState.get(msg.channel).add(msg.id);
            }
            state.add(msg.channel, msg.id);
        }
        static State copy(State from) {
            State result = new State();
            for (String c : List.of("A", "B")) result.applied.get(c).addAll(from.applied.get(c));
            return result;
        }
        void read(Socket socket, String expected) {
            try (socket; BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                require(("HELLO " + expected).equals(in.readLine()), "Handshake failed: " + expected);
                String line;
                while ((line = in.readLine()) != null) {
                    String[] parts = line.split(" ");
                    if (parts.length == 1 && parts[0].equals("END")) { queue.put(new Msg(expected,"END",0)); return; }
                    require(parts.length == 2 && (parts[0].equals("EVENT") || parts[0].equals("BARRIER")), "Bad TCP frame " + line);
                    queue.put(new Msg(expected, parts[0], Integer.parseInt(parts[1])));
                }
                throw new EOFException("Input " + expected + " closed without END");
            } catch (Throwable e) { errors.add(e); }
        }
        void serve(boolean recovering) throws Exception {
            ExecutorService readers = Executors.newFixedThreadPool(2);
            try (ServerSocket a = new ServerSocket(PORT_A, 1, InetAddress.getLoopbackAddress());
                 ServerSocket b = new ServerSocket(PORT_B, 1, InetAddress.getLoopbackAddress())) {
                a.setSoTimeout(10000); b.setSoTimeout(10000);
                System.out.println("READY " + mode + " " + (recovering ? "RECOVER" : "INITIAL")); System.out.flush();
                Socket sa = a.accept(), sb = b.accept();
                readers.submit(() -> read(sa, "A")); readers.submit(() -> read(sb, "B"));
                int ends = 0;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (ends < 2 && System.nanoTime() < deadline) {
                    Throwable error = errors.peek(); if (error != null) throw new IOException("Reader failed", error);
                    Msg msg = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (msg == null) continue;
                    if (msg.kind.equals("END")) ends++; else if (!recovering) handle(msg); else {
                        require(msg.kind.equals("EVENT"), "Recovery accepts events only");
                        state.add(msg.channel, msg.id);
                    }
                }
                require(ends == 2, "Timed out receiving source streams");
                require(errors.isEmpty(), "TCP reader error " + errors.peek());
                if (recovering) {
                    for (String c : List.of("A", "B")) {
                        require(state.applied.get(c).size() == TOTAL, "Wrong count on " + c);
                        for (int i = 1; i <= TOTAL; i++) require(state.applied.get(c).contains(i), "Missing " + c + i);
                    }
                    System.out.println("RECOVERY_PASS total=" + state.count() + " restored_checkpoint=20 replayed_post_checkpoint=20");
                } else require(complete, "Checkpoint not created");
                System.out.flush();
            } finally { readers.shutdownNow(); readers.awaitTermination(2, TimeUnit.SECONDS); }
        }
    }
    static void send(int port, String channel, boolean recovering, BlockingQueue<Throwable> errors) {
        try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            socket.setTcpNoDelay(true);
            out.println("HELLO " + channel);
            if (!recovering) {
                for (int i = 1; i <= BEFORE; i++) {
                    out.println("EVENT " + i);
                    Thread.sleep(3);
                    if (channel.equals("B") && i == 5) Thread.sleep(300);
                }
                out.println("BARRIER 1");
                if (channel.equals("A")) {
                    for (int i = BEFORE + 1; i <= TOTAL; i++) { out.println("EVENT " + i); Thread.sleep(3); }
                }
                // Source B sends post-barrier events only after restart. Source A's
                // post-barrier events before crash are intentionally replayed.
            } else {
                for (int i = BEFORE + 1; i <= TOTAL; i++) { out.println("EVENT " + i); Thread.sleep(3); }
            }
            out.println("END");
            if (out.checkError()) throw new IOException("Source write failed " + channel);
        } catch (Throwable t) { errors.add(t); }
    }
    static class Child implements AutoCloseable {
        final Process process;
        final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        Child(Mode mode, boolean recovering) throws Exception {
            String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
            process = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                DurableMultiInputRecoveryExperiment.class.getName(), "worker", mode.name(),
                recovering ? "recover" : "initial").redirectErrorStream(true).start();
            Thread t = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String s; while ((s = reader.readLine()) != null) lines.add(s);
                } catch (IOException e) { lines.add("ERROR " + e); }
            }, "child-output");
            t.setDaemon(true); t.start();
        }
        String await(String prefix) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                String line = lines.poll(100, TimeUnit.MILLISECONDS);
                if (line != null) {
                    if (line.startsWith(prefix)) return line;
                    if (line.startsWith("ERROR")) throw new IOException(line);
                    System.out.println("  worker: " + line);
                }
                if (!process.isAlive() && lines.isEmpty()) throw new IOException("Worker exited early: " + process.exitValue());
            }
            throw new TimeoutException("Waiting for worker output: " + prefix);
        }
        public void close() throws Exception {
            if (process.isAlive()) process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
    }
    static void phase(Mode mode, boolean recovering, Child child) throws Exception {
        child.await("READY ");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        BlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
        try {
            Future<?> a = pool.submit(() -> send(PORT_A, "A", recovering, errors));
            Future<?> b = pool.submit(() -> send(PORT_B, "B", recovering, errors));
            a.get(10, TimeUnit.SECONDS); b.get(10, TimeUnit.SECONDS);
            require(errors.isEmpty(), "Source error " + errors.peek());
            System.out.println("  " + child.await(recovering ? "RECOVERY_PASS" : "CHECKPOINT_DURABLE"));
        } finally { pool.shutdownNow(); pool.awaitTermination(2, TimeUnit.SECONDS); }
    }
    static void experiment(Mode mode) throws Exception {
        Files.createDirectories(DIR); Files.deleteIfExists(file(mode));
        System.out.println("\n===== " + mode + " =====");
        try (Child initial = new Child(mode, false)) {
            phase(mode, false, initial);
            require(Files.size(file(mode)) > 0, "Checkpoint missing on disk");
            System.out.println("  *** KILLING MERGE WORKER JVM AFTER DURABLE CHECKPOINT ***");
            initial.process.destroyForcibly();
            require(initial.process.waitFor(5, TimeUnit.SECONDS), "Could not kill worker");
        }
        try (Child recovered = new Child(mode, true)) {
            phase(mode, true, recovered);
            require(recovered.process.waitFor(5, TimeUnit.SECONDS) && recovered.process.exitValue() == 0,
                "Recovered worker did not exit successfully");
        }
        System.out.println(mode + " RESULT: PASS");
    }
    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("worker")) {
            new Worker(Mode.valueOf(args[1]), args[2].equals("recover")).serve(args[2].equals("recover"));
            return;
        }
        System.out.println("STAGE 9: DURABLE MULTI-INPUT CHECKPOINT + REAL JVM CRASH/RECOVERY");
        System.out.println("Source B stalls 300ms; both sources have 20 events; checkpoint after event 10");
        experiment(Mode.ALIGNED);
        experiment(Mode.UNALIGNED);
        System.out.println("\nSTAGE 9 COMPLETE: ALIGNED PASS | UNALIGNED PASS");
        System.out.println("Checkpoint files: " + DIR.toAbsolutePath());
    }
}
