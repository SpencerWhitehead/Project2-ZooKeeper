import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by spencerwhitehead on 11/18/16.
 */
public class Election {
    private boolean holdingElection = false;
    private int coord = -1;
    private ConcurrentSkipListSet<Integer> okay;

    public Election() {
        okay = new ConcurrentSkipListSet<>();
    }

    public synchronized boolean recvdCoord() { return coord != -1; }

    public synchronized int getCoord() { return coord; }

    public synchronized int getNumOkays() { return okay.size();}

    public synchronized boolean ongoingElection() { return holdingElection; }

    public synchronized void setCoord(int nodeID) { coord = nodeID; }

    public synchronized void holdElection() { holdingElection = true; }

    public synchronized void addOkay(int nodeID) { okay.add(nodeID); }

    public synchronized void endElection() {
        holdingElection = false;
        coord = -1;
        okay.clear();
    }
}
