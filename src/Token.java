/**
 * Spencer Whitehead, whites5
 * Vipula Rawte, rawtev
 */

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/* Class to represent a token (file) in the system. */
public class Token {
    private String fname; // File name
    private int holder;  // ID of holder node
    private boolean asked = false; // Requested resource
    private boolean inUse = false; // Using resource
    private Queue<Integer> reqQ = new ConcurrentLinkedQueue<>(); // Request queue
    String contents = ""; // File contents

    public Token(String f, int hold) {
        this.fname = f;
        this.holder = hold;
    }

    /* Retrieve file name. */
    public String getFname() {return fname;}

    /* Retrieve holder ID. */
    public int getHolder() {return holder;}

    /* Retrieve requested resource. */
    public boolean getAsked() {return asked;}

    /* Retreive using resource variable. */
    public boolean getInUse() {return inUse;}

    /* Retrieve contents of file. */
    public String getContents() {return contents;}

    /* Determine if request queue is empty. */
    public boolean isReqQEmpty() {return reqQ.isEmpty();}

    /* Set flag for using resource. */
    public void setInUse(boolean use) {inUse = use;}

    /* Set flag for requested resource. */
    public void setAsked(boolean ask) {asked = ask;}

    /* Set holder of this token. */
    public void setHolder(int hold) {holder = hold;}

    /* Set contents of file. */
    public void setContents(String data) {contents = data;}

    /* Add node to requeset queue. */
    public void request(int nodeID) {reqQ.add(nodeID);}

    /* Retrieve next node ID from request queue. */
    public Integer deq() {return reqQ.poll();}

    /* Append data to file. */
    public void appendContents(String toAppend) {
        StringBuilder s = new StringBuilder();
        s.append(contents);
        s.append(toAppend);
        contents = s.toString();
    }

    /* Delete contents of file. */
    public void releaseContents() {contents = "";}
}
