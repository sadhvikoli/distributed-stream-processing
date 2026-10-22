
package edu.project.stream;

import java.io.*;
import java.net.*;

public class Source {

    private final Object sendLock = new Object();
    private final PrintWriter pipelineWriter;

    public Source(PrintWriter pipelineWriter) {
        this.pipelineWriter = pipelineWriter;
    }

    private void sendEvent(long eventId, String event)
            throws IOException {

        synchronized (sendLock) {
            pipelineWriter.println(
                    "DATA " + eventId + ":" + event);

            if (pipelineWriter.checkError()) {
                throw new IOException("Pipeline write failed");
            }

            System.out.println(
                    "Source sent: " + eventId + ":" + event);
        }
    }

    private void injectBarrier(long checkpointId)
            throws IOException {

        synchronized (sendLock) {
            pipelineWriter.println(
                    "BARRIER " + checkpointId);

            if (pipelineWriter.checkError()) {
                throw new IOException("Barrier write failed");
            }

            System.out.println(
                    "Source injected barrier " + checkpointId);
        }
    }

    public static void main(String[] args) throws Exception {

        try (Socket coordinatorSocket =
                     new Socket("localhost", 6060);
             Socket workerSocket =
                     new Socket("localhost", 5050);
             PrintWriter coordinatorWriter =
                     new PrintWriter(
                             coordinatorSocket.getOutputStream(), true);
             BufferedReader coordinatorReader =
                     new BufferedReader(
                             new InputStreamReader(
                                     coordinatorSocket.getInputStream()));
             PrintWriter pipelineWriter =
                     new PrintWriter(
                             workerSocket.getOutputStream(), true)) {

            Source source = new Source(pipelineWriter);

            coordinatorWriter.println("HELLO Source");

            System.out.println(
                    "Source connected to Coordinator and Worker-1.");

            Thread barrierListener = new Thread(() -> {

                try {
                    String message;

                    while ((message =
                            coordinatorReader.readLine()) != null) {

                        if (message.startsWith("CHECKPOINT ")) {
                            long checkpointId = Long.parseLong(
                                    message.substring(11));

                            source.injectBarrier(checkpointId);
                        }
                    }

                } catch (Exception e) {
                    System.out.println(
                            "Source control connection ended: "
                                    + e.getMessage());
                }
            });

            barrierListener.setDaemon(true);
            barrierListener.start();

            String[] events = {
                    "apple", "banana", "apple",
                    "orange", "banana", "apple",
                    "grape", "apple", "orange",
                    "banana", "grape", "apple"
            };

            long eventId = 1;

            for (String event : events) {
                source.sendEvent(eventId++, event);
                Thread.sleep(1000);
            }

            System.out.println(
                    "All events sent. Source remains available "
                            + "for checkpoint barriers.");

            // Keep the data connection open for checkpoint tests.
            barrierListener.join();
        }
    }
}
