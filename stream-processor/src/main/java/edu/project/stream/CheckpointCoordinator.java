
package edu.project.stream;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class CheckpointCoordinator {

    private final Map<String, PrintWriter> connections =
            new ConcurrentHashMap<>();

    private final Set<String> expectedWorkers =
            Set.of("Worker-1", "Worker-2", "Worker-3");

    private final Map<Long, Set<String>> acknowledgments =
            new ConcurrentHashMap<>();

    private final Map<Long, Long> startTimes =
            new ConcurrentHashMap<>();

    private final Set<Long> completed =
            ConcurrentHashMap.newKeySet();

    private long nextCheckpointId = 0;

    private void handleConnection(Socket socket) {
        String participant = null;

        try (socket;
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(socket.getInputStream()));
             PrintWriter writer = new PrintWriter(
                     socket.getOutputStream(), true)) {

            String hello = reader.readLine();

            if (hello == null || !hello.startsWith("HELLO ")) {
                return;
            }

            participant = hello.substring(6);

            if (!expectedWorkers.contains(participant)
                    && !participant.equals("Source")) {
                System.out.println("Rejected: " + participant);
                return;
            }

            if (connections.putIfAbsent(participant, writer) != null) {
                System.out.println("Duplicate connection: " + participant);
                return;
            }

            System.out.println("Registered: " + participant);

            String message;

            while ((message = reader.readLine()) != null) {
                String[] parts = message.split(" ");

                if (parts.length == 2 && parts[0].equals("ACK")
                        && expectedWorkers.contains(participant)) {

                    long checkpointId = Long.parseLong(parts[1]);
                    acknowledge(participant, checkpointId);
                }
            }

        } catch (Exception e) {
            System.out.println("Connection ended: " + e.getMessage());
        } finally {
            if (participant != null) {
                connections.remove(participant);
                System.out.println("Disconnected: " + participant);
            }
        }
    }

    private synchronized void acknowledge(
            String workerId, long checkpointId) {

        Set<String> acked = acknowledgments.get(checkpointId);

        if (acked == null || completed.contains(checkpointId)) {
            return;
        }

        if (!acked.add(workerId)) {
            return;
        }

        System.out.println(
                workerId + " acknowledged checkpoint " + checkpointId);

        if (acked.containsAll(expectedWorkers)) {
            completed.add(checkpointId);

            double durationMs =
                    (System.nanoTime() - startTimes.get(checkpointId))
                            / 1_000_000.0;

            System.out.println(
                    "GLOBAL CHECKPOINT " + checkpointId + " COMPLETED");
            System.out.printf(
                    "Checkpoint duration: %.3f ms%n", durationMs);
        }
    }

    private synchronized void startCheckpoint() {

        if (!connections.keySet().containsAll(expectedWorkers)
                || !connections.containsKey("Source")) {
            System.out.println(
                    "Cannot checkpoint: not all participants connected.");
            return;
        }

        for (Map.Entry<Long, Set<String>> entry
                : acknowledgments.entrySet()) {
            if (!completed.contains(entry.getKey())) {
                System.out.println(
                        "Previous checkpoint still in progress.");
                return;
            }
        }

        long checkpointId = ++nextCheckpointId;

        acknowledgments.put(
                checkpointId, ConcurrentHashMap.newKeySet());

        startTimes.put(checkpointId, System.nanoTime());

        System.out.println(
                "Starting checkpoint " + checkpointId);

        // Source injects the barrier into the ordered data stream.
        connections.get("Source").println(
                "CHECKPOINT " + checkpointId);
    }

    public static void main(String[] args) throws Exception {

        CheckpointCoordinator coordinator =
                new CheckpointCoordinator();

        try (ServerSocket server = new ServerSocket(6060)) {

            System.out.println(
                    "Coordinator listening on port 6060...");

            Thread acceptThread = new Thread(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket socket = server.accept();

                        Thread handler = new Thread(
                                () -> coordinator.handleConnection(socket));

                        handler.setDaemon(true);
                        handler.start();

                    } catch (IOException e) {
                        break;
                    }
                }
            });

            acceptThread.setDaemon(true);
            acceptThread.start();

            BufferedReader console = new BufferedReader(
                    new InputStreamReader(System.in));

            System.out.println(
                    "Start all Workers and Source.");
            System.out.println(
                    "Press ENTER to initiate a checkpoint.");

            while (console.readLine() != null) {
                coordinator.startCheckpoint();
            }
        }
    }
}
