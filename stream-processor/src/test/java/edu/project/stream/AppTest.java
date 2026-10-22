package edu.project.stream;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AppTest {

    @Test
    void duplicateEventDoesNotChangeWorkerState() throws Exception {

        Worker worker = new Worker("Test-Worker");

        Method processEvent = Worker.class.getDeclaredMethod(
                "processEvent", String.class);
        processEvent.setAccessible(true);

        processEvent.invoke(worker, "1:apple");
        processEvent.invoke(worker, "2:apple");
        processEvent.invoke(worker, "2:apple");

        Field countsField = Worker.class.getDeclaredField("counts");
        countsField.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Integer> counts = (Map<String, Integer>) countsField.get(worker);

        assertEquals(2, counts.get("apple"));
    }

    @Test
    void olderEventDoesNotChangeWorkerState() throws Exception {

        Worker worker = new Worker("Test-Worker");

        Method processEvent = Worker.class.getDeclaredMethod(
                "processEvent", String.class);
        processEvent.setAccessible(true);

        processEvent.invoke(worker, "1:apple");
        processEvent.invoke(worker, "2:banana");
        processEvent.invoke(worker, "3:apple");

        // An older event arrives after event 3.
        processEvent.invoke(worker, "2:banana");

        Field countsField = Worker.class.getDeclaredField("counts");
        countsField.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Integer> counts = (Map<String, Integer>) countsField.get(worker);

        assertEquals(2, counts.get("apple"));
        assertEquals(1, counts.get("banana"));
    }

    @Test
    void checkpointPersistsCorrectWorkerState() throws Exception {

        String workerId = "JUnit-" + java.util.UUID.randomUUID();
        long checkpointId = 1L;

        Worker worker = new Worker(workerId);

        Method processEvent = Worker.class.getDeclaredMethod(
                "processEvent", String.class);
        processEvent.setAccessible(true);

        processEvent.invoke(worker, "1:apple");
        processEvent.invoke(worker, "2:banana");
        processEvent.invoke(worker, "3:apple");

        Method saveCheckpoint = Worker.class.getDeclaredMethod(
                "saveCheckpoint", long.class);
        saveCheckpoint.setAccessible(true);

        java.nio.file.Path checkpoint = java.nio.file.Path.of(
                "checkpoints",
                workerId + "-checkpoint-" + checkpointId + ".dat");

        try {
            saveCheckpoint.invoke(worker, checkpointId);

            try (java.io.ObjectInputStream input = new java.io.ObjectInputStream(
                    java.nio.file.Files.newInputStream(checkpoint))) {

                Object savedCounts = input.readObject();
                long savedEventId = input.readLong();

                assertEquals(
                        Map.of("apple", 2, "banana", 1),
                        savedCounts);

                assertEquals(3L, savedEventId);
            }
        } finally {
            java.nio.file.Files.deleteIfExists(checkpoint);
        }
    }

}
