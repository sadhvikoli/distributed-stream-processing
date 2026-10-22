
package edu.project.stream;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Stage 5: Automated distributed recovery experiment.
 *
 * Features:
 * - Three real Worker JVM processes
 * - TCP communication
 * - Durable checkpoints
 * - Automatic Worker-2 crash injection
 * - Automatic process restart
 * - Checkpoint restoration
 * - Event replay
 * - Final correctness verification
 */
public class RecoveryExperiment {

    static final int EVENTS = 60;
    static final int EVERY = 10;
    static final int CRASH_AT = 27;

    static final int BASE_PORT = 18500;

    static final Path DIR =
        Paths.get("recovery-experiment-data");

    static final String[] FRUITS = {
        "apple", "banana", "apple", "orange", "grape"
    };

    // ==========================================
    // WORKER STATE
    // ==========================================

    static class State implements Serializable {

        private static final long serialVersionUID = 1L;

        int last = 0;

        Map<String, Integer> counts =
            new TreeMap<>();
    }

    // ==========================================
    // SAVE CHECKPOINT
    // ==========================================

    static void save(Path path, State state)
            throws Exception {

        Files.createDirectories(path.getParent());

        Path temp = path.resolveSibling(
            path.getFileName() + ".tmp"
        );

        try (ObjectOutputStream out =
                 new ObjectOutputStream(
                     Files.newOutputStream(temp))) {

            out.writeObject(state);
        }

        try {
            Files.move(
                temp,
                path,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
            );
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(
                temp,
                path,
                StandardCopyOption.REPLACE_EXISTING
            );
        }
    }

    // ==========================================
    // LOAD CHECKPOINT
    // ==========================================

    static State load(Path path) throws Exception {

        try (ObjectInputStream in =
                 new ObjectInputStream(
                     Files.newInputStream(path))) {

            return (State) in.readObject();
        }
    }

    // ==========================================
    // TCP ACKNOWLEDGMENT VALIDATION
    // ==========================================

    static void requireAck(
            BufferedReader reader,
            String expected) throws Exception {

        String actual = reader.readLine();

        if (!expected.equals(actual)) {
            throw new IOException(
                "Expected " + expected
                + ", but received " + actual
            );
        }
    }

    // ==========================================
    // CONNECT TO WORKER
    // ==========================================

    static Socket connect(int port)
            throws Exception {

        for (int attempt = 0;
             attempt < 100;
             attempt++) {

            try {
                Socket socket = new Socket();

                socket.connect(
                    new InetSocketAddress(
                        "127.0.0.1",
                        port
                    ),
                    500
                );

                socket.setSoTimeout(15000);

                return socket;

            } catch (IOException e) {
                Thread.sleep(50);
            }
        }

        throw new IOException(
            "Unable to connect to port " + port
        );
    }

    // ==========================================
    // WORKER PROCESS
    // ==========================================

    static void worker(
            int index,
            int restoredCheckpoint) throws Exception {

        State state;

        if (restoredCheckpoint == 0) {
            state = new State();
        } else {
            state = load(
                DIR.resolve(
                    "worker-" + index
                    + "-" + restoredCheckpoint
                    + ".dat"
                )
            );
        }

        try (ServerSocket server =
                 new ServerSocket(BASE_PORT + index)) {

            server.setReuseAddress(true);

            System.out.printf(
                "Worker-%d listening | restored event %d%n",
                index,
                state.last
            );

            try (
                Socket upstream = server.accept();

                BufferedReader reader =
                    new BufferedReader(
                        new InputStreamReader(
                            upstream.getInputStream()
                        )
                    );

                PrintWriter ack =
                    new PrintWriter(
                        upstream.getOutputStream(),
                        true
                    )
            ) {

                Socket downstream = null;
                PrintWriter forward = null;
                BufferedReader downstreamAck = null;

                try {

                    if (index < 3) {

                        downstream = connect(
                            BASE_PORT + index + 1
                        );

                        forward = new PrintWriter(
                            downstream.getOutputStream(),
                            true
                        );

                        downstreamAck =
                            new BufferedReader(
                                new InputStreamReader(
                                    downstream.getInputStream()
                                )
                            );
                    }

                    String line;

                    while ((line = reader.readLine())
                            != null) {

                        // --------------------------
                        // PROCESS EVENT
                        // --------------------------

                        if (line.startsWith("EVENT ")) {

                            String[] parts =
                                line.split(" ", 3);

                            int eventId =
                                Integer.parseInt(parts[1]);

                            String fruit = parts[2];

                            if (eventId > state.last) {

                                if (eventId
                                        != state.last + 1) {

                                    throw new
                                        IllegalStateException(
                                            "Worker-" + index
                                            + " missing event "
                                            + eventId
                                        );
                                }

                                state.counts.merge(
                                    fruit,
                                    1,
                                    Integer::sum
                                );

                                state.last = eventId;
                            }

                            if (forward != null) {

                                forward.println(line);

                                requireAck(
                                    downstreamAck,
                                    "EVENT_OK " + eventId
                                );
                            }

                            ack.println(
                                "EVENT_OK " + eventId
                            );
                        }

                        // --------------------------
                        // CHECKPOINT
                        // --------------------------

                        else if (line.startsWith(
                                "CHECKPOINT ")) {

                            int checkpointId =
                                Integer.parseInt(
                                    line.substring(11)
                                );

                            save(
                                DIR.resolve(
                                    "worker-" + index
                                    + "-" + checkpointId
                                    + ".dat"
                                ),
                                state
                            );

                            if (forward != null) {

                                forward.println(line);

                                requireAck(
                                    downstreamAck,
                                    "CHECKPOINT_OK "
                                    + checkpointId
                                );
                            }

                            ack.println(
                                "CHECKPOINT_OK "
                                + checkpointId
                            );
                        }

                        // --------------------------
                        // FINISH PROCESSING
                        // --------------------------

                        else if (line.equals("END")) {

                            if (forward != null) {

                                forward.println("END");

                                requireAck(
                                    downstreamAck,
                                    "END_OK"
                                );
                            }

                            save(
                                DIR.resolve(
                                    "worker-" + index
                                    + "-final.dat"
                                ),
                                state
                            );

                            ack.println("END_OK");

                            break;
                        }

                        else {
                            throw new
                                IllegalArgumentException(
                                    "Unknown message: "
                                    + line
                                );
                        }
                    }

                } finally {

                    if (downstream != null) {
                        downstream.close();
                    }
                }
            }
        }
    }

    // ==========================================
    // CLUSTER MANAGEMENT
    // ==========================================

    static class Cluster implements AutoCloseable {

        final List<Process> processes =
            new ArrayList<>();

        Socket source;
        PrintWriter send;
        BufferedReader ack;

        Cluster(int restoredCheckpoint)
                throws Exception {

            String javaExecutable =
                Paths.get(
                    System.getProperty("java.home"),
                    "bin",
                    "java"
                ).toString();

            String classpath =
                System.getProperty("java.class.path");

            // Start downstream workers first.
            for (int index = 3;
                 index >= 1;
                 index--) {

                Process process =
                    new ProcessBuilder(
                        javaExecutable,
                        "-cp",
                        classpath,
                        RecoveryExperiment.class.getName(),
                        "worker",
                        Integer.toString(index),
                        Integer.toString(
                            restoredCheckpoint
                        )
                    )
                    .inheritIO()
                    .start();

                processes.add(process);
            }

            source = connect(BASE_PORT + 1);

            send = new PrintWriter(
                source.getOutputStream(),
                true
            );

            ack = new BufferedReader(
                new InputStreamReader(
                    source.getInputStream()
                )
            );
        }

        // --------------------------
        // SEND EVENT
        // --------------------------

        void event(int eventId) throws Exception {

            String fruit =
                FRUITS[(eventId - 1) % FRUITS.length];

            send.println(
                "EVENT " + eventId + " " + fruit
            );

            requireAck(
                ack,
                "EVENT_OK " + eventId
            );
        }

        // --------------------------
        // TRIGGER CHECKPOINT
        // --------------------------

        void checkpoint(int checkpointId)
                throws Exception {

            send.println(
                "CHECKPOINT " + checkpointId
            );

            requireAck(
                ack,
                "CHECKPOINT_OK " + checkpointId
            );
        }

        // --------------------------
        // FINISH STREAM
        // --------------------------

        void finish() throws Exception {

            send.println("END");

            requireAck(ack, "END_OK");
        }

        // --------------------------
        // STOP WORKERS
        // --------------------------

        @Override
        public void close() {

            if (source != null) {
                try {
                    source.close();
                } catch (IOException ignored) {
                }
            }

            for (Process process : processes) {
                process.destroyForcibly();
            }

            for (Process process : processes) {

                try {
                    process.waitFor(
                        2,
                        TimeUnit.SECONDS
                    );

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // ==========================================
    // COORDINATOR
    // ==========================================

    static void coordinator() throws Exception {

        Files.createDirectories(DIR);

        // Clear previous experiment files.
        try (java.util.stream.Stream<Path> paths =
                 Files.list(DIR)) {

            for (Path path : paths.toList()) {
                Files.delete(path);
            }
        }

        int latestCompleted = 0;
        int checkpoints = 0;
        int replayed = 0;

        boolean failureInjected = false;

        long experimentStart =
            System.nanoTime();

        Cluster cluster = null;

        try {

            cluster = new Cluster(0);

            for (int eventId = 1;
                 eventId <= EVENTS;
                 eventId++) {

                cluster.event(eventId);

                // --------------------------
                // AUTOMATIC CHECKPOINT
                // --------------------------

                if (eventId % EVERY == 0) {

                    long checkpointStart =
                        System.nanoTime();

                    cluster.checkpoint(eventId);

                    Files.writeString(
                        DIR.resolve("completed.txt"),
                        Integer.toString(eventId)
                    );

                    latestCompleted = eventId;
                    checkpoints++;

                    double checkpointMs =
                        (System.nanoTime()
                            - checkpointStart)
                        / 1_000_000.0;

                    System.out.printf(
                        Locale.US,
                        "GLOBAL CHECKPOINT at event %d "
                        + "completed in %.2f ms%n",
                        eventId,
                        checkpointMs
                    );
                }

                // --------------------------
                // AUTOMATIC WORKER CRASH
                // --------------------------

                if (!failureInjected
                        && eventId == CRASH_AT) {

                    failureInjected = true;

                    System.out.println();
                    System.out.println(
                        "*** KILLING Worker-2 "
                        + "at event " + eventId
                        + " ***"
                    );

                    // Processes started in order:
                    // Worker-3, Worker-2, Worker-1
                    Process failedWorker =
                        cluster.processes.get(1);

                    failedWorker.destroyForcibly();
                    failedWorker.waitFor();

                    // Stop remaining workers.
                    cluster.close();

                    System.out.println(
                        "Restarting all workers from "
                        + "checkpoint event "
                        + latestCompleted
                    );

                    // Restart the complete pipeline.
                    cluster = new Cluster(
                        latestCompleted
                    );

                    // --------------------------
                    // EVENT REPLAY
                    // --------------------------

                    for (int replayId =
                             latestCompleted + 1;
                         replayId <= eventId;
                         replayId++) {

                        cluster.event(replayId);
                        replayed++;
                    }

                    System.out.println(
                        "RECOVERY COMPLETED"
                    );

                    System.out.println(
                        "Events replayed: "
                        + replayed
                    );

                    System.out.println();
                }
            }

            cluster.finish();

        } finally {

            if (cluster != null) {
                cluster.close();
            }
        }

        // ======================================
        // FINAL CORRECTNESS VERIFICATION
        // ======================================

        Map<String, Integer> expected =
            new TreeMap<>();

        for (int eventId = 1;
             eventId <= EVENTS;
             eventId++) {

            String fruit =
                FRUITS[(eventId - 1) % FRUITS.length];

            expected.merge(
                fruit,
                1,
                Integer::sum
            );
        }

        boolean pass = true;

        System.out.println();
        System.out.println(
            "========== FINAL WORKER STATES =========="
        );

        for (int index = 1;
             index <= 3;
             index++) {

            State state = load(
                DIR.resolve(
                    "worker-" + index
                    + "-final.dat"
                )
            );

            boolean correct =
                state.last == EVENTS
                && state.counts.equals(expected);

            System.out.printf(
                "Worker-%d | last event=%d "
                + "| counts=%s | correctness=%s%n",
                index,
                state.last,
                state.counts,
                correct ? "PASS" : "FAIL"
            );

            pass &= correct;
        }

        double elapsedMs =
            (System.nanoTime()
                - experimentStart)
            / 1_000_000.0;

        System.out.println();
        System.out.println(
            "========== RECOVERY RESULTS =========="
        );

        System.out.println(
            "Total events: " + EVENTS
        );

        System.out.println(
            "Completed checkpoints: "
            + checkpoints
        );

        System.out.println(
            "Worker crashes: 1"
        );

        System.out.println(
            "Events replayed: "
            + replayed
        );

        System.out.printf(
            Locale.US,
            "Total elapsed time: %.2f ms%n",
            elapsedMs
        );

        System.out.println(
            "Correctness: "
            + (pass ? "PASS" : "FAIL")
        );

        if (!pass) {
            throw new IllegalStateException(
                "Recovery correctness failed"
            );
        }
    }

    // ==========================================
    // MAIN
    // ==========================================

    public static void main(String[] args)
            throws Exception {

        if (args.length > 0
                && args[0].equals("worker")) {

            int workerIndex =
                Integer.parseInt(args[1]);

            int restoredCheckpoint =
                Integer.parseInt(args[2]);

            worker(
                workerIndex,
                restoredCheckpoint
            );

        } else {

            coordinator();
        }
    }
}
