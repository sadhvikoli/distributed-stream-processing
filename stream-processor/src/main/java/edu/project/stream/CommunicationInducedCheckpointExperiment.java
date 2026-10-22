package edu.project.stream;

import java.util.*;

/** Stage 11: deterministic communication-induced checkpoint prototype.
 * Each message piggybacks sender checkpoint epoch. A receiver with an older
 * epoch takes a forced checkpoint BEFORE recording the receive. This is an
 * educational epoch-based protocol, NOT an implementation of HMNR.
 */
public final class CommunicationInducedCheckpointExperiment {
    static final int N = 3;
    static final class Checkpoint {
        final int epoch, position;
        final String reason;
        Checkpoint(int e,int p,String r){epoch=e;position=p;reason=r;}
    }
    static final class Message {
        final String id; final int from,to,sendPos,receivePos,senderEpoch;
        Message(String id,int f,int t,int sp,int rp,int e){this.id=id;from=f;to=t;sendPos=sp;receivePos=rp;senderEpoch=e;}
    }
    static final class Run {
        final String mode;
        final int[] positions=new int[N],epochs=new int[N];
        final List<Checkpoint>[] checkpoints;
        final List<Message> messages=new ArrayList<>();
        int forced=0, voluntary=0;
        @SuppressWarnings("unchecked") Run(String mode){
            this.mode=mode;checkpoints=new List[N];
            for(int p=0;p<N;p++){checkpoints[p]=new ArrayList<>();checkpoints[p].add(new Checkpoint(0,0,"initial"));}
        }
        void checkpoint(int p,String reason){
            epochs[p]++;
            checkpoints[p].add(new Checkpoint(epochs[p],positions[p],reason));
            if(reason.equals("forced"))forced++;else voluntary++;
            System.out.printf("  P%d CHECKPOINT epoch=%d position=%d (%s)%n",p+1,epochs[p],positions[p],reason);
        }
        void send(String id,int from,int to){
            int sp=++positions[from];int sentEpoch=epochs[from];
            if(mode.equals("INDUCED") && epochs[to]<sentEpoch){
                // Force the receiver to record a cut BEFORE it receives this message.
                while(epochs[to]<sentEpoch) checkpoint(to,"forced");
            }
            int rp=++positions[to];
            messages.add(new Message(id,from,to,sp,rp,sentEpoch));
            System.out.printf("  %s P%d -> P%d (piggyback epoch=%d)%n",id,from+1,to+1,sentEpoch);
        }
        void scenario(){
            // Identical application messages and voluntary checkpoints across modes.
            checkpoint(0,"voluntary");
            send("m1",0,1);
            checkpoint(1,"voluntary");
            send("m2",1,2);
            checkpoint(2,"voluntary");
            send("m3",2,0);
            send("m4",0,2);
            send("m5",2,1);
            checkpoint(0,"voluntary");
            send("m6",0,1);
        }
        boolean consistent(int[] choice){
            for(Message m:messages){
                Checkpoint s=checkpoints[m.from].get(choice[m.from]);
                Checkpoint r=checkpoints[m.to].get(choice[m.to]);
                if(r.position>=m.receivePos && s.position<m.sendPos)return false;
            }
            return true;
        }
        String describe(int[] c){
            StringJoiner j=new StringJoiner(" | ");
            for(int p=0;p<N;p++){
                Checkpoint cp=checkpoints[p].get(c[p]);
                j.add("P"+(p+1)+":e"+cp.epoch+"@"+cp.position);
            }
            return j.toString();
        }
        int[] recover(int[] latest){
            int[] selected=latest.clone();
            int rounds=0;
            while(!consistent(selected)){
                boolean[] rollback=new boolean[N];
                for(Message m:messages){
                    Checkpoint s=checkpoints[m.from].get(selected[m.from]);
                    Checkpoint r=checkpoints[m.to].get(selected[m.to]);
                    if(r.position>=m.receivePos && s.position<m.sendPos){
                        System.out.printf("  orphan %s: P%d receive present, P%d send absent%n",m.id,m.to+1,m.from+1);
                        rollback[m.to]=true;
                    }
                }
                boolean changed=false;
                for(int p=0;p<N;p++)if(rollback[p]){
                    if(selected[p]==0)throw new AssertionError("Inconsistent initial state");
                    selected[p]--;changed=true;
                }
                if(!changed || ++rounds>100)throw new AssertionError("Recovery failed");
            }
            return selected;
        }
        void evaluate(){
            int[] latest=new int[N];
            for(int p=0;p<N;p++)latest[p]=checkpoints[p].size()-1;
            // Fault: newest checkpoint on P1 is unavailable.
            latest[0]--;
            System.out.println("  Fault: newest P1 checkpoint unavailable");
            System.out.println("  Candidates: "+describe(latest));
            int[] selected=recover(latest);
            int rollbackSteps=0, replay=0;
            for(int p=0;p<N;p++){
                rollbackSteps+=latest[p]-selected[p];
                replay+=positions[p]-checkpoints[p].get(selected[p]).position;
            }
            System.out.println("  Recovery: "+describe(selected));
            System.out.printf("  Forced checkpoints=%d | voluntary=%d | extra rollback steps=%d | trace events to replay=%d%n",forced,voluntary,rollbackSteps,replay);
            if(!consistent(selected))throw new AssertionError("Inconsistent recovery");
            System.out.println("  CORRECTNESS: PASS");
            System.out.printf("RESULT %-14s forced=%d rollback=%d replay=%d PASS%n",mode,forced,rollbackSteps,replay);
        }
    }
    public static void main(String[] args){
        System.out.println("STAGE 11: COMMUNICATION-INDUCED CHECKPOINT PROTOTYPE");
        System.out.println("Epoch piggyback + receiver-side forced checkpoints; deterministic in-memory trace.");
        for(String mode:List.of("UNCOORDINATED","INDUCED")){
            System.out.println("\n===== "+mode+" =====");
            Run r=new Run(mode);r.scenario();r.evaluate();
        }
        System.out.println("\nSTAGE 11 COMPLETE: BOTH RECOVERY LINES CONSISTENT");
        System.out.println("Note: epoch forcing alone does NOT guarantee domino-effect freedom or implement HMNR.");
    }
}
