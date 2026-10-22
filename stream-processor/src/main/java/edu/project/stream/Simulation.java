
package edu.project.stream;

import java.util.*;

public class Simulation {

    private static final int TOTAL_EVENTS = 1000;
    private static final int WORKER_COUNT = 3;

    private static final int[] FAILURE_EVENTS = {
        173, 427, 811
    };

    private static final int FIXED_INTERVAL = 10;
    private static final int MIN_INTERVAL = 5;
    private static final int MAX_INTERVAL = 20;

    // Explicit simulation cost model, in milliseconds.
    // These are assumptions, NOT measured network/disk times.
    private static final double EVENT_COST_MS = 0.30;
    private static final double CHECKPOINT_BASE_MS = 3.0;
    private static final double CHECKPOINT_STATE_COST_MS = 0.005;
    private static final double RESTART_COST_MS = 2.0;

    private static final String[] EVENTS = {
        "apple", "banana", "apple", "orange", "grape"
    };

    enum Policy {
        FIXED, ADAPTIVE
    }

    static class Snapshot {
        final int eventId;
        final Map<String, Integer> counts;

        Snapshot(int eventId, Map<String, Integer> counts) {
            this.eventId = eventId;
            this.counts = new HashMap<>(counts);
        }
    }

    static class Worker {
        final String name;
        final Map<String, Integer> counts = new HashMap<>();

        int lastEventId = 0;

        Worker(String name) {
            this.name = name;
        }

        void process(int eventId, String event) {
            if (eventId <= lastEventId) {
                return;
            }

            if (eventId != lastEventId + 1) {
                throw new IllegalStateException(
                    name + " received out-of-order event "
                    + eventId
                );
            }

            counts.merge(event, 1, Integer::sum);
            lastEventId = eventId;
        }

        Snapshot snapshot() {
            return new Snapshot(lastEventId, counts);
        }

        void restore(Snapshot snapshot) {
            counts.clear();
            counts.putAll(snapshot.counts);
            lastEventId = snapshot.eventId;
        }
    }

    static class Result {
        Policy policy;

        int checkpoints;
        int failures;
        int replayedEvents;
        int finalInterval;

        double checkpointOverheadMs;
        double recoveryTimeMs;
        double processingTimeMs;
        double totalTimeMs;
        double throughput;

        boolean correct;
    }

    static class Experiment {

        final Policy policy;

        final List<Worker> workers = new ArrayList<>();

        List<Snapshot> latestSnapshots =
            new ArrayList<>();

        int checkpointId = 0;
        int replayedEvents = 0;
        int failures = 0;

        int interval = FIXED_INTERVAL;
        int nextCheckpointAt = FIXED_INTERVAL;
        int checkpointsSinceFailure = 0;

        double checkpointOverheadMs = 0;
        double recoveryTimeMs = 0;
        double processingTimeMs = 0;

        Experiment(Policy policy) {
            this.policy = policy;

            for (int i = 1; i <= WORKER_COUNT; i++) {
                workers.add(new Worker("Worker-" + i));
            }

            // Initial globally consistent state at event 0.
            for (Worker worker : workers) {
                latestSnapshots.add(worker.snapshot());
            }
        }

        String eventAt(int id) {
            return EVENTS[(id - 1) % EVENTS.length];
        }

        void processEvent(int id) {
            String event = eventAt(id);

            // Logical pipeline: all workers process the
            // same ordered event.
            for (Worker worker : workers) {
                worker.process(id, event);
            }

            processingTimeMs += EVENT_COST_MS;
        }

        void checkpoint(int eventId) {

            checkpointId++;

            List<Snapshot> newSnapshots =
                new ArrayList<>();

            // Ordered barrier: each worker snapshots
            // after processing eventId.
            for (Worker worker : workers) {
                newSnapshots.add(worker.snapshot());
            }

            latestSnapshots = newSnapshots;

            // Modeled checkpoint overhead.
            double cost =
                CHECKPOINT_BASE_MS
                + CHECKPOINT_STATE_COST_MS * eventId;

            checkpointOverheadMs += cost;

            System.out.printf(
                Locale.US,
                "[%s] Checkpoint %d at event %d "
                + "| interval=%d | modeled cost=%.2f ms%n",
                policy,
                checkpointId,
                eventId,
                interval,
                cost
            );

            if (policy == Policy.ADAPTIVE) {
                checkpointsSinceFailure++;

                // Gradually reduce checkpoint frequency
                // during stable execution.
                if (checkpointsSinceFailure >= 3) {
                    interval = Math.min(
                        MAX_INTERVAL,
                        interval + 2
                    );

                    checkpointsSinceFailure = 0;
                }
            }

            nextCheckpointAt = eventId + interval;
        }

        void simulateFailure(int failedAtEvent) {

            failures++;

            int restoredEvent =
                latestSnapshots.get(0).eventId;

            int replayCount =
                failedAtEvent - restoredEvent;

            System.out.println();
            System.out.println(
                "[" + policy + "] SIMULATED FAILURE "
                + failures + " at event " + failedAtEvent
            );

            // Restore the complete logical pipeline.
            for (int i = 0; i < WORKER_COUNT; i++) {
                workers.get(i).restore(
                    latestSnapshots.get(i)
                );
            }

            // Modeled restart cost.
            recoveryTimeMs += RESTART_COST_MS;

            // Replay events that occurred after
            // the last completed checkpoint.
            for (int id = restoredEvent + 1;
                 id <= failedAtEvent; id++) {

                processEvent(id);
                replayedEvents++;
            }

            // Replay processing cost is already included
            // in processingTimeMs. Recovery time includes
            // that cost for reporting purposes.
            recoveryTimeMs +=
                replayCount * EVENT_COST_MS;

            System.out.println(
                "[" + policy + "] Restored event "
                + restoredEvent
                + " | Replayed " + replayCount + " events"
            );

            if (policy == Policy.ADAPTIVE) {

                // Simple feedback-based policy:
                // more replay means more frequent checkpoints.
                if (replayCount >= 5) {
                    interval = MIN_INTERVAL;
                } else {
                    interval = Math.max(
                        MIN_INTERVAL,
                        interval - 2
                    );
                }

                checkpointsSinceFailure = 0;
                nextCheckpointAt =
                    failedAtEvent + interval;

                System.out.println(
                    "[" + policy + "] Adaptive interval "
                    + "adjusted to " + interval
                );
            }

            System.out.println();
        }

        boolean validate() {

            Map<String, Integer> expected =
                new HashMap<>();

            for (int id = 1; id <= TOTAL_EVENTS; id++) {
                expected.merge(
                    eventAt(id),
                    1,
                    Integer::sum
                );
            }

            for (Worker worker : workers) {
                if (worker.lastEventId != TOTAL_EVENTS
                    || !worker.counts.equals(expected)) {

                    return false;
                }
            }

            return true;
        }

        Result run() {

            System.out.println();
            System.out.println(
                "========== " + policy
                + " CHECKPOINTING =========="
            );

            for (int id = 1; id <= TOTAL_EVENTS; id++) {

                processEvent(id);

                // Failure is injected before any checkpoint
                // that might be due at the same event.
                for (int failure : FAILURE_EVENTS) {
                    if (id == failure) {
                        simulateFailure(id);
                    }
                }

                if (id >= nextCheckpointAt) {
                    checkpoint(id);
                }
            }

            Result result = new Result();

            result.policy = policy;
            result.checkpoints = checkpointId;
            result.failures = failures;
            result.replayedEvents = replayedEvents;
            result.finalInterval = interval;

            result.checkpointOverheadMs =
                checkpointOverheadMs;

            result.recoveryTimeMs =
                recoveryTimeMs;

            result.processingTimeMs =
                processingTimeMs;

            // Recovery processing is already counted
            // in processingTimeMs. Add only restart cost.
            double restartOverhead =
                failures * RESTART_COST_MS;

            result.totalTimeMs =
                processingTimeMs
                + checkpointOverheadMs
                + restartOverhead;

            result.throughput =
                TOTAL_EVENTS / (result.totalTimeMs / 1000.0);

            result.correct = validate();

            if (!result.correct) {
                throw new IllegalStateException(
                    policy + " correctness FAILED"
                );
            }

            return result;
        }
    }

    static void printComparison(
            Result fixed,
            Result adaptive) {

        System.out.println();
        System.out.println(
            "=============================================="
        );
        System.out.println(
            "       CHECKPOINT POLICY COMPARISON"
        );
        System.out.println(
            "=============================================="
        );

        System.out.printf(
            "%-27s %12s %12s%n",
            "Metric", "FIXED", "ADAPTIVE"
        );

        System.out.println(
            "----------------------------------------------------"
        );

        System.out.printf(
            "%-27s %12d %12d%n",
            "Completed checkpoints",
            fixed.checkpoints,
            adaptive.checkpoints
        );

        System.out.printf(
            "%-27s %12d %12d%n",
            "Simulated failures",
            fixed.failures,
            adaptive.failures
        );

        System.out.printf(
            "%-27s %12d %12d%n",
            "Replayed events",
            fixed.replayedEvents,
            adaptive.replayedEvents
        );

        System.out.printf(
            Locale.US,
            "%-27s %12.2f %12.2f%n",
            "Checkpoint overhead (ms)",
            fixed.checkpointOverheadMs,
            adaptive.checkpointOverheadMs
        );

        System.out.printf(
            Locale.US,
            "%-27s %12.2f %12.2f%n",
            "Recovery time (ms)",
            fixed.recoveryTimeMs,
            adaptive.recoveryTimeMs
        );

        System.out.printf(
            Locale.US,
            "%-27s %12.2f %12.2f%n",
            "Total modeled time (ms)",
            fixed.totalTimeMs,
            adaptive.totalTimeMs
        );

        System.out.printf(
            Locale.US,
            "%-27s %12.2f %12.2f%n",
            "Throughput (events/sec)",
            fixed.throughput,
            adaptive.throughput
        );

        System.out.printf(
            "%-27s %12s %12s%n",
            "Correctness",
            fixed.correct ? "PASS" : "FAIL",
            adaptive.correct ? "PASS" : "FAIL"
        );

        System.out.println(
            "----------------------------------------------------"
        );

        double improvement =
            (fixed.totalTimeMs - adaptive.totalTimeMs)
            / fixed.totalTimeMs * 100.0;

        System.out.printf(
            Locale.US,
            "Adaptive total-time improvement: %.2f%%%n",
            improvement
        );

        if (improvement > 0) {
            System.out.println(
                "Adaptive checkpointing performed better "
                + "under this simulated cost model."
            );
        } else if (improvement < 0) {
            System.out.println(
                "Fixed checkpointing performed better "
                + "under this simulated cost model."
            );
        } else {
            System.out.println(
                "Both policies had equal modeled total time."
            );
        }

        System.out.println(
            "All durations are modeled costs, "
            + "not wall-clock measurements."
        );
    }

    public static void main(String[] args) {

        System.out.println(
            "AUTOMATED CHECKPOINT POLICY EXPERIMENT"
        );

        System.out.println(
            "Events: " + TOTAL_EVENTS
            + " | Workers: " + WORKER_COUNT
            + " | Failure events: "
            + Arrays.toString(FAILURE_EVENTS)
        );

        Result fixed =
            new Experiment(Policy.FIXED).run();

        Result adaptive =
            new Experiment(Policy.ADAPTIVE).run();

        printComparison(fixed, adaptive);
    }
}
