
package edu.project.stream;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

public class Worker {

    private final String workerId;
    private final Map<String, Integer> counts = new HashMap<>();

    private long lastProcessedEventId = 0;

    public Worker(String workerId) {
        this.workerId = workerId;
    }

    private void processEvent(String message) {

        String[] parts = message.split(":", 2);

        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid event: " + message);
        }

        long eventId = Long.parseLong(parts[0]);
        String event = parts[1];

        if (eventId <= lastProcessedEventId) {
            System.out.println(
                    workerId + " skipping duplicate event " + eventId);
            return;
        }

        counts.put(event, counts.getOrDefault(event, 0) + 1);
        lastProcessedEventId = eventId;

        System.out.println(
                workerId + " processed " + message
                        + " | State: " + counts);
    }

    private void saveCheckpoint(long checkpointId)
            throws IOException {

        Path directory = Path.of("checkpoints");
        Files.createDirectories(directory);

        Path destination = directory.resolve(
                workerId + "-checkpoint-" + checkpointId + ".dat");

        Path temporary = directory.resolve(
                workerId + "-checkpoint-" + checkpointId + ".tmp");

        try (ObjectOutputStream output =
                     new ObjectOutputStream(
                             Files.newOutputStream(temporary))) {

            output.writeObject(new HashMap<>(counts));
            output.writeLong(lastProcessedEventId);
        }

        try {
            Files.move(
                    temporary,
                    destination,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException(
                    "Atomic checkpoint replacement unsupported", e);
        }

        System.out.println(
                workerId + " saved checkpoint " + checkpointId
                        + " | State: " + counts
                        + " | Last Event: " + lastProcessedEventId);
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println(
                    "Usage: Worker <id> <port> <nextPort|none>");
            return;
        }

        String workerId = args[0];
        int listenPort = Integer.parseInt(args[1]);
        String nextPortArgument = args[2];

        Worker worker = new Worker(workerId);

        try (Socket coordinatorSocket =
                     new Socket("localhost", 6060);
             PrintWriter coordinatorWriter =
                     new PrintWriter(
                             coordinatorSocket.getOutputStream(), true);
             BufferedReader coordinatorReader =
                     new BufferedReader(
                             new InputStreamReader(
                                     coordinatorSocket.getInputStream()));
             ServerSocket server =
                     new ServerSocket(listenPort)) {

            coordinatorWriter.println("HELLO " + workerId);

            // Detect Coordinator disconnection.
            Thread monitor = new Thread(() -> {
                try {
                    while (coordinatorReader.readLine() != null) {
                        // Workers receive barriers via their data input.
                    }
                } catch (IOException ignored) {
                }

                System.out.println(
                        workerId + ": Coordinator disconnected.");
                System.exit(1);
            });

            monitor.setDaemon(true);
            monitor.start();

            Socket downstreamSocket = null;
            PrintWriter downstreamWriter = null;

            try {
                if (!nextPortArgument.equalsIgnoreCase("none")) {
                    int nextPort = Integer.parseInt(nextPortArgument);

                    downstreamSocket =
                            new Socket("localhost", nextPort);

                    downstreamWriter = new PrintWriter(
                            downstreamSocket.getOutputStream(), true);

                    System.out.println(
                            workerId + " connected downstream on "
                                    + nextPort);
                }

                System.out.println(
                        workerId + " listening on port " + listenPort);

                try (Socket upstreamSocket = server.accept();
                     BufferedReader upstreamReader =
                             new BufferedReader(
                                     new InputStreamReader(
                                             upstreamSocket.getInputStream()))) {

                    System.out.println(
                            workerId + ": Upstream connected!");

                    String line;

                    while ((line = upstreamReader.readLine()) != null) {

                        if (line.startsWith("DATA ")) {

                            String event = line.substring(5);
                            worker.processEvent(event);

                            if (downstreamWriter != null) {
                                downstreamWriter.println(line);
                                if (downstreamWriter.checkError()) {
                                    throw new IOException(
                                            "Downstream write failed");
                                }
                            }

                        } else if (line.startsWith("BARRIER ")) {

                            long checkpointId = Long.parseLong(
                                    line.substring(8));

                            System.out.println(
                                    workerId + " received barrier "
                                            + checkpointId);

                            worker.saveCheckpoint(checkpointId);

                            // Forward barrier only after local snapshot.
                            if (downstreamWriter != null) {
                                downstreamWriter.println(line);

                                if (downstreamWriter.checkError()) {
                                    throw new IOException(
                                            "Barrier forwarding failed");
                                }
                            }

                            coordinatorWriter.println(
                                    "ACK " + checkpointId);

                            if (coordinatorWriter.checkError()) {
                                throw new IOException(
                                        "Coordinator ACK failed");
                            }

                        } else {
                            System.out.println(
                                    workerId + " unknown message: " + line);
                        }
                    }

                    System.out.println(
                            workerId + ": Upstream disconnected.");
                }

            } finally {
                if (downstreamSocket != null) {
                    downstreamSocket.close();
                }
            }
        }
    }
}
