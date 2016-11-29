import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by spencerwhitehead on 11/18/16.
 */
public class Election {
    private boolean holdingElection = false;
    private int coord = -1;
    private ConcurrentSkipListSet<Integer> okay;
    private ConcurrentHashMap<Integer, ZXID> responders;

    public Election() {
        okay = new ConcurrentSkipListSet<>();
        responders = new ConcurrentHashMap<>();
    }

    public synchronized boolean recvdCoord() { return coord != -1; }

    public synchronized boolean recvdOkay(int nodeID) { return okay.contains(nodeID); }

    public synchronized int getCoord() { return coord; }

    public synchronized int getNumOkays() { return okay.size(); }

    public synchronized ZXID getResponderZXID(int nodeID) { return responders.get(nodeID); }

    public synchronized boolean ongoingElection() { return holdingElection; }

    public synchronized void setCoord(int nodeID) { coord = nodeID; }

    public synchronized void holdElection() {
        holdingElection = true;
        coord = -1;
        okay.clear();
        responders.clear();
    }

    public synchronized void addOkay(int nodeID) { okay.add(nodeID); }

    public synchronized void addResponder(int nodeEpoch, int nodeCounter, int nodeID) {
        responders.put(nodeID, new ZXID(nodeEpoch, nodeCounter));
    }

    public synchronized void endElection() {
        holdingElection = false;
//        coord = -1;
//        okay.clear();
//        responders.clear();
    }
}
