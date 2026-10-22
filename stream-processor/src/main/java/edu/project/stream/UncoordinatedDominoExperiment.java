package edu.project.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Stage 10: deterministic event-trace experiment for uncoordinated checkpoints.
 * Local checkpoints are modeled in memory; no JVM crash or disk durability is claimed.
 */
public final class UncoordinatedDominoExperiment {
    private static final int PROCESSES = 3;
    private static final class Event {
        final int process;
        final String description;
        Event(int process, String description) { this.process = process; this.description = description; }
    }
    private static final class Message {
        final String id;
        final int sender, sendPosition, receiver, receivePosition;
        Message(String id, int sender, int sendPosition, int receiver, int receivePosition) {
            this.id = id; this.sender = sender; this.sendPosition = sendPosition;
            this.receiver = receiver; this.receivePosition = receivePosition;
        }
    }
    private static final class Checkpoint {
        final String name;
        final int position;
        Checkpoint(String name, int position) { this.name = name; this.position = position; }
    }
    private static final class Trace {
        final List<Event>[] events;
        final List<Checkpoint>[] checkpoints;
        final List<Message> messages = new ArrayList<>();
        @SuppressWarnings("unchecked")
        Trace() {
            events = new List[PROCESSES]; checkpoints = new List[PROCESSES];
            for (int p = 0; p < PROCESSES; p++) {
                events[p] = new ArrayList<>();
                checkpoints[p] = new ArrayList<>();
                checkpoints[p].add(new Checkpoint("C0", 0));
            }
        }
        int event(int process, String description) {
            events[process].add(new Event(process, description));
            return events[process].size();
        }
        void send(String id, int sender, int receiver) {
            int send = event(sender, "SEND " + id + " -> P" + (receiver + 1));
            int receive = event(receiver, "RECEIVE " + id + " <- P" + (sender + 1));
            messages.add(new Message(id, sender, send, receiver, receive));
        }
        void checkpoint(int process, String name) {
            checkpoints[process].add(new Checkpoint(name, events[process].size()));
        }
    }
    private static boolean consistent(Trace trace, int[] selected) {
        for (Message m : trace.messages) {
            int recvCut = trace.checkpoints[m.receiver].get(selected[m.receiver]).position;
            int sendCut = trace.checkpoints[m.sender].get(selected[m.sender]).position;
            if (recvCut >= m.receivePosition && sendCut < m.sendPosition) return false;
        }
        return true;
    }
    private static String line(Trace trace, int[] selected) {
        StringBuilder out = new StringBuilder();
        for (int p = 0; p < PROCESSES; p++) {
            if (p > 0) out.append(" | ");
            Checkpoint cp = trace.checkpoints[p].get(selected[p]);
            out.append("P").append(p + 1).append(":").append(cp.name)
               .append("(events=").append(cp.position).append(")");
        }
        return out.toString();
    }
    private static int[] recover(Trace trace, int[] latest) {
        int[] selected = latest.clone();
        int round = 0;
        System.out.println("Initial recovery candidates: " + line(trace, selected));
        while (true) {
            round++;
            boolean[] mustRollback = new boolean[PROCESSES];
            for (Message m : trace.messages) {
                Checkpoint recv = trace.checkpoints[m.receiver].get(selected[m.receiver]);
                Checkpoint send = trace.checkpoints[m.sender].get(selected[m.sender]);
                if (recv.position >= m.receivePosition && send.position < m.sendPosition) {
                    System.out.printf("  ORPHAN RECEIVE: %s in P%d:%s, but SEND absent from P%d:%s%n",
                        m.id, m.receiver + 1, recv.name, m.sender + 1, send.name);
                    mustRollback[m.receiver] = true;
                }
            }
            boolean changed = false;
            for (int p = 0; p < PROCESSES; p++) {
                if (mustRollback[p]) {
                    if (selected[p] == 0) throw new IllegalStateException("Initial checkpoint inconsistent");
                    String before = trace.checkpoints[p].get(selected[p]).name;
                    selected[p]--;
                    System.out.printf("  Round %d: P%d rolls back %s -> %s%n", round, p + 1,
                        before, trace.checkpoints[p].get(selected[p]).name);
                    changed = true;
                }
            }
            if (!changed) break;
        }
        if (!consistent(trace, selected)) throw new AssertionError("Recovery line inconsistent");
        System.out.println("Consistent recovery line: " + line(trace, selected));
        return selected;
    }
    private static Trace buildDominoTrace() {
        Trace t = new Trace();
        // Each process takes its local checkpoint after receiving a message.
        // P1's later checkpoint is deliberately made unavailable at failure time.
        t.checkpoint(0, "C1"); // P1 has not sent m1 yet.
        t.send("m1", 0, 1);
        t.checkpoint(1, "C1"); // P2 records receiving m1.
        t.send("m2", 1, 2);
        t.checkpoint(2, "C1"); // P3 records receiving m2.
        t.send("m3", 2, 0);
        t.checkpoint(0, "C2"); // P1's newest local checkpoint, unavailable after fault.
        return t;
    }
    private static void domino() {
        System.out.println("\n=== EXPERIMENT A: UNCOORDINATED CHECKPOINTS / DOMINO EFFECT ===");
        Trace t = buildDominoTrace();
        int[] latest = {1, 1, 1}; // P1 C2 unavailable; P2/P3 retain their latest C1.
        System.out.println("Injected fault: P1's C2 is unavailable; P1 must restore C1.");
        int[] recovered = recover(t, latest);
        int[] expected = {1, 0, 0};
        if (!Arrays.equals(recovered, expected)) throw new AssertionError("Unexpected recovery line");
        int lost = 0;
        for (int p = 0; p < PROCESSES; p++) {
            int latestPosition = t.events[p].size();
            int recoveredPosition = t.checkpoints[p].get(recovered[p]).position;
            lost += latestPosition - recoveredPosition;
        }
        System.out.printf("Rollback reaches initial state on P2 and P3; %d trace events must be replayed.%n", lost);
        System.out.println("DOMINO EFFECT: PASS");
    }
    private static void coordinated() {
        System.out.println("\n=== EXPERIMENT B: COORDINATED CONSISTENT RECOVERY LINE ===");
        Trace t = buildDominoTrace();
        // The globally consistent cut is taken after all three messages were sent and received.
        for (int p = 0; p < PROCESSES; p++) t.checkpoint(p, "GLOBAL");
        int[] global = {t.checkpoints[0].size() - 1, t.checkpoints[1].size() - 1,
                        t.checkpoints[2].size() - 1};
        if (!consistent(t, global)) throw new AssertionError("Coordinated checkpoint inconsistent");
        System.out.println("Committed coordinated checkpoint: " + line(t, global));
        int[] recovered = recover(t, global);
        if (!Arrays.equals(recovered, global)) throw new AssertionError("Unexpected coordinated rollback");
        System.out.println("No cascading rollback required.");
        System.out.println("COORDINATED RECOVERY: PASS");
    }
    public static void main(String[] args) {
        System.out.println("STAGE 10: RECOVERY LINES AND THE DOMINO EFFECT");
        System.out.println("Deterministic message-dependency trace; local checkpoints modeled in memory.");
        domino();
        coordinated();
        System.out.println("\nSTAGE 10 COMPLETE: DOMINO PASS | COORDINATED PASS");
    }
}
