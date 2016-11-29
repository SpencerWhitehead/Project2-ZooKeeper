/**
 * Created by spencerwhitehead on 11/28/16.
 */
public class ZXID {
    private int epoch;
    private int counter;

    public ZXID(int currentEpoch, int currentCounter) {
        epoch = currentEpoch;
        counter = currentCounter;
    }

    public int getEpoch() { return epoch; }

    public int getCounter() { return counter; }

    public void setEpoch(int e) { epoch = e; }

    public void setCounter(int c) { counter = c; }

    public synchronized void updateEpoch() {
        epoch++;
        counter = 0;
    }

    public synchronized void updateCounter() { counter++; }

    public boolean greaterThan(int myNodeID, int nodeEpoch, int nodeCount, int nodeID) {
        if (epoch > nodeEpoch) { return true; }
        else if (epoch == nodeEpoch) {
            if (counter > nodeCount) { return true; }
            else if (counter == nodeCount) { return myNodeID > nodeID; }
        }
        return false;
    }
}
