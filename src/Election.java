import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by spencerwhitehead on 11/18/16.
 */
public class Election {
    private boolean holdingElection = false;
    private int coord = -1;
    private int recognizedEnd = 0;
    private int numElects = 0;
//    private ConcurrentHashMap<Integer, Boolean> okay;
    private ConcurrentSkipListSet<Integer> okay;

    public Election() {
        okay = new ConcurrentSkipListSet<>();
    }

    public synchronized boolean recvdCoord() { return coord != -1; }

    public synchronized int getCoord() { return coord; }

    public synchronized boolean recvdOkay(int nodeID) { return okay.contains(nodeID); }

    public synchronized int getNumOkays() { return okay.size();}

    public synchronized boolean ongoingElection() { return holdingElection; }

//    public synchronized boolean ongoingElection() { return holdingElection && numElects!=0; }

    public synchronized void setCoord(int nodeID) { coord = nodeID; }

    public synchronized void holdElection() { holdingElection = true; }

//    public synchronized void addElectHolder() { numElects++; }

    public synchronized void addOkay(int nodeID) { okay.add(nodeID); }

    public synchronized void endElection() { holdingElection = false; }

//    public synchronized void endElection() {
//        if(recognizedEnd == 0) {
//            holdingElection = false;
//            okay.clear();
//        }
//        recognizedEnd++;
//        if(recognizedEnd == numElects) {
//            coord = -1;
//            recognizedEnd = 0;
//            numElects = 0;
//        }
//    }

//    public synchronized void endElection(int numActiveNodes) {
//        if(recognizedEnd == 0) {
//            holdingElection = false;
//            okay.clear();
//        }
//        recognizedEnd++;
//        if(recognizedEnd == numActiveNodes) {
//            coord = -1;
//            recognizedEnd = 0;
//        }
//    }
}
