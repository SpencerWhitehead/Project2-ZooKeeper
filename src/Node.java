/**
 * Spencer Whitehead, whites5
 * Partha Sarathi Mukherjee, mukhep
 */

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/*
*  Class to perform functionalities of a node in Raymond's algorithm.
*  Throughout this file and others, the words token and file are used
*  interchangeably. As in each file is a token and is represented by a
*  token object.
*/
public class Node {
    private int portNum;	// Port number on which node will be listening to accept connections
    private int ID;	        // ID of node
    private int leaderID = -1;
    private ZXID zxid = new ZXID(0,0);
    private String hist = "history.txt";
    private String initSend;
    private Election elect;
    private HashMap<Integer, AddrPair> neighbors = new HashMap<>(); // Map to store IP addresses and
                                                                    // port numbers of neighbor nodes.
    private ConcurrentHashMap<Integer, Socket> connections = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, ZXID> processIDs = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Token> tokens = new ConcurrentHashMap<>(); // Map to store token objects.
    private ConcurrentHashMap<String, Queue<String[]>> commands = new ConcurrentHashMap<>(); // Map to
                                                                                            // store what commands
                                                                                            // should be ran on each file.

    public Node(int port, int ident) {
        this.portNum = port;
        this.ID = ident;
        StringBuilder s = new StringBuilder();
        s.append("UP|");
        s.append(ID);
        s.append("|");
        initSend = s.toString();
        elect = new Election();
        initializeHistFile();
    }

    private void initializeHistFile() {
        File f = new File(hist);
        try {
            f.createNewFile();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Parse incoming message. */
    private String[] parseMsg(String msg){ return msg.split("\\|"); }

    /* Create file. */
    private void createFile(String fname) {
        if (!tokens.containsKey(fname)) {
            Token t = new Token(fname);
            tokens.put(fname, t);
            System.out.println("\tCreated file: "+fname);
            StringBuilder s = new StringBuilder();
            s.append("\tNumber of tokens: ");
            s.append(tokens.size());
            System.out.println(s.toString());
        }
        else {
            System.err.println("\tError: file already exists, "+fname);
        }
    }

    /* Delete file. */
    private void deleteFile(String fname) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.remove(fname);
            Queue q = commands.remove(fname);
            System.out.println("\tDeleted file: "+fname);
            StringBuilder s = new StringBuilder();
            s.append("\tNumber of tokens: ");
            s.append(tokens.size());
            System.out.println(s.toString());
        }
        else {
            System.err.println("\tError: no such file, "+fname);
        }
    }

    /* Append to specified file. */
    private void appendFile(String fname, String toAdd) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.get(fname);
            t.appendContents(toAdd);
            tokens.put(fname, t);
            System.out.println("\tAppended to file: "+fname);
        }
        else {
            System.err.println("\tError: no such file, "+fname);
        }
    }

    /* Read file specified file. */
    private void readFile(String fname) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.get(fname);
            System.out.println("\tReading "+fname+":");
            System.out.println("\t\t"+tokens.get(fname).getContents());
        }
        else {
            System.err.println("\tError: no such file, "+fname);
        }
    }

    private synchronized String buildHistEntry(String[] m) {
        StringBuilder s = new StringBuilder();
        s.append(m[m.length-2]);
        s.append("|");
        s.append(m[m.length-1]);
        s.append("|");
        int i;
        for(i=1; i<m.length-2; i++) { s.append(m[i]); s.append("|"); }
        return s.toString();
    }

    private synchronized void updateHistory(String[] msg) {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(hist, true));
            bw.write(buildHistEntry(msg));
            bw.newLine();
            bw.flush();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            if (bw != null) try {
                bw.close();
            } catch (IOException ioe2) {
                ioe2.printStackTrace();
            }
        }
    }

    private boolean toSend(int nodeID, int criteria) {
        if (criteria == 0) { return nodeID != ID; }

        ZXID temp = elect.getResponderZXID(nodeID);
        if (temp != null) {
            if (criteria == -1) {
                return zxid.greaterThan(ID, temp.getEpoch(), temp.getCounter(), nodeID, leaderID);
            } else if (criteria == 1) {
                return !zxid.greaterThan(ID, temp.getEpoch(), temp.getCounter(), nodeID, leaderID);
            }
        }
        return false;
    }

    /*
    *  whichNdoes == 1 means send to nodes with higher IDs
    *  whichNodes == 0 means send to all other nodes
    *  whichNodes == -1 means send to nodes with lower IDs
    *  whichNodes == -2 means send to only the node specified by nodeID
    *  */
    private void sendToNodes(String[] contents, int nodeID, int whichNodes) {
        String msg = MessageSender.formatMsg(contents);
        if(nodeID != 0 && whichNodes == -2 && connections.containsKey(nodeID)) {
            System.out.println("ABOUT TO SEND " +contents[0]+" MESSAGE TO "+Integer.toString(nodeID));
            MessageSender.sendMsg(connections.get(nodeID), msg);
        }
        else {
            for (Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
                if (toSend(entry.getKey(), whichNodes) && connections.containsKey(entry.getKey())) {
                    MessageSender.sendMsg(connections.get(entry.getKey()), msg);
                }
            }
        }
    }

    private String[] createLeaderElectMsg(String com) {
        return new String[] {com, Integer.toString(zxid.getEpoch()), Integer.toString(zxid.getCounter()), Integer.toString(ID)};
    }

    private void initElection() {
        System.out.println("ATTEMPTING ELECTION");
        elect.holdElection();
        sendToNodes(createLeaderElectMsg("ELE"), 0, 0);
        try {
            Thread.sleep(750);
//            Thread.sleep(1000);
//            if(elect.getNumOkays() == 0) {
            if(elect.getNumOkays() == 0 && elect.ongoingElection()) {
                System.out.println("No responses... Guess I'm the leader");
                if (leaderID != ID) {
                    leaderID = ID;
                    zxid.updateEpoch();
                }
                sendToNodes(createLeaderElectMsg("COR"), 0, -1);
                elect.endElection();
            }
            else {
//                Thread.sleep(1500);
                Thread.sleep(1000);
                if(elect.recvdCoord()) {
                    leaderID = elect.getCoord();
                }
                else if (elect.ongoingElection()) { initElection(); }
//                else { initElection(); }
            }
        }
        catch (InterruptedException e) {
            System.err.println("Initiate election error:");
            System.err.println(e);
        }
    }

    private void onElectRecv(int nodeEpoch, int nodeCounter, int nodeID) {
        System.out.println("SENDING OKAY MESSAGE");
        StringBuilder s1 = new StringBuilder();
        s1.append(ID);
        s1.append("==>");
        s1.append(zxid.getEpoch());
        s1.append(",");
        s1.append(zxid.getCounter());
        System.out.println(s1.toString());

        StringBuilder s2 = new StringBuilder();
        s2.append(nodeID);
        s2.append("==>");
        s2.append(nodeEpoch);
        s2.append(",");
        s2.append(nodeCounter);
        System.out.println(s2.toString());

        if(zxid.greaterThan(ID, nodeEpoch, nodeCounter, nodeID, leaderID)) {
            sendToNodes(createLeaderElectMsg("OKA"), nodeID, -2);
            if(!elect.ongoingElection()) { initElection(); }
        }
        else {
            sendToNodes(createLeaderElectMsg("NOK"), nodeID, -2);
        }
    }

    private void onCoordRecv(int nodeEpoch, int nodeCount, int nodeID) {
        if(zxid.greaterThan(ID, nodeEpoch, nodeCount, nodeID, leaderID) && !elect.ongoingElection()) { initElection(); }
        else {
            elect.setCoord(nodeID);
            if (leaderID != nodeID) {
                leaderID = nodeID;
                zxid.setEpoch(nodeEpoch);
                zxid.setCounter(nodeCount);
            }
            System.out.println(Thread.currentThread().getName()+"==>NEW LEADER IS: " + Integer.toString(leaderID));
            elect.endElection();
        }
    }

    /*
        election ID ==> ELE|<epoch>|<counter>|ID|
		coordinator ID ==> COR|<epoch>|<counter>|ID|
		ok ID ==> OKA|<epoch>|<counter>|ID|
     */
    private class ElectHandler implements Runnable {
        private String[] msg;
        public ElectHandler(String[] m) { msg = m; }

        @Override
        public void run() {
            int nodeEpoch = Integer.parseInt(msg[1]);
            int nodeCounter = Integer.parseInt(msg[2]);
            int n = Integer.parseInt(msg[3]);
            switch (msg[0]) {
                case "ELE":
                    Node.this.onElectRecv(nodeEpoch, nodeCounter, n);
                    break;
                case "COR":
                    Node.this.onCoordRecv(nodeEpoch, nodeCounter, n);
                    break;
                case "OKA":
                    Node.this.elect.addOkay(n);
                    Node.this.elect.addResponder(nodeEpoch, nodeCounter, n);
                    break;
                case "NOK":
                    Node.this.elect.addResponder(nodeEpoch, nodeCounter, n);
                    break;
            }
        }
    }

    /* Class to handle incoming messages. */
    private class ConnectHandler implements Runnable {
        private Socket socket = null; // Socket of incoming connection.
        private BufferedReader is = null; // Buffer to read incoming message.
        private PrintWriter os = null;
        private int connID = -1;
        public ConnectHandler(Socket sock) { socket = sock; }

        /* Parse and perform actions based on message. */
        private void handleMsg(String msg) {
            String[] m = Node.this.parseMsg(msg);
            switch (m[0]){
                /* If NEW is keyword, then create file. */
                case "NEW":
                    if(!Node.this.tokens.containsKey(m[2])) {
                        System.out.println("\tCreating file: "+m[2]);
                        Node.this.createFile(m[2]);
                    }
                    break;
                /* If DEL is keyword, then delete file. */
                case "DEL":
                    if(Node.this.tokens.containsKey(m[2])) {
                        System.out.println("\tDeleting file: "+m[2]);
                        Node.this.deleteFile(m[2]);
                    }
                    break;
                /* If REQ is keyword, then request token. */
                case "APP":
                    System.out.println("\tReceived request for file: "+m[2]);
                    break;
                case "RED":
                    System.out.println("\tReceived token: "+m[2]);
                    break;
                case "ELE":
                    System.out.println("Received election message from: "+m[3]);
                    Thread electThread = new Thread(new ElectHandler(m));
                    electThread.start();
                    break;
                case "COR":
                    System.out.println("Received coordinator message from: "+m[3]);
                    Thread coordinateThread = new Thread(new ElectHandler(m));
                    coordinateThread.start();
                    break;
                case "OKA":
                    System.out.println("Received OK message from: "+m[3]);
                    Thread okayThread = new Thread(new ElectHandler(m));
                    okayThread.start();
                    break;
                case "NOK":
                    System.out.println("Received NOK message from: "+m[3]);
                    Thread notOkayThread = new Thread(new ElectHandler(m));
                    notOkayThread.start();
                    break;
                case "UP":
                    connID = Integer.parseInt(m[1]);
                    if(!Node.this.connections.containsKey(connID)) {
                        Node.this.connections.put(connID, socket);
                        System.out.println("Added NodeID "+connID+" to connections");
                    }
                    break;
                default:
                    System.err.println("\tInvalid message: "+msg);
                    break;
            }
        }

        /* Read in and handle message. */
        @Override
        public void run() {
            try {
                is = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                os = new PrintWriter(socket.getOutputStream(), true);
                os.println(initSend);
                while (true) {
                    String msg = is.readLine();
                    if(msg == null) {
                        break;
                    }
                    System.out.println("\tReceived: " + msg);
                    handleMsg(msg);
                }
                if (connID != -1) {
                    System.out.println("LOST NODE CONNECTION TO "+Integer.toString(connID));
                    Node.this.connections.remove(connID);
                    System.out.println(Node.this.connections.size());
                    /* If connID == leaderID, then initiate leader election */
                    if(connID == leaderID) {
                        leaderID = -1;
                        Node.this.initElection();
                    }
                }
                else {
                    System.out.println("LOST CLIENT CONNECTION");
                }
            }
            catch (IOException e){
                System.err.println("Connection error (ConnectHandler):");
                System.err.println(e);
            }
        }
    }

    /* Start server and accept connections. Each connection is handled in a thread. */
    public void begin() {
        Runnable serverTask = new Runnable() {
            @Override
            public void run() {

                try {
                    ServerSocket serverSocket = new ServerSocket(portNum);

                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        Thread clientThread = new Thread(new ConnectHandler(clientSocket));
                        clientThread.start();
                    }
                } catch (IOException e) {
                    System.err.println("Accept failed.");
                }
            }
        };
        Thread serverThread = new Thread(serverTask);
        serverThread.start();
        for(Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
            if(entry.getKey() != ID) {
                if (!connections.containsKey(entry.getKey())) {
                    try {
                        AddrPair loc = entry.getValue();
                        Socket sock = new Socket(loc.addr, loc.port);
                        connections.put(entry.getKey(),sock);
                        Thread connThread = new Thread(new ConnectHandler(connections.get(entry.getKey())));
                        connThread.start();
                    }
                    catch (IOException e) {
                        System.err.println("Neighbor connection error: ");
                        System.err.println(e);
                    }
                }
            }
        }
        System.out.println("INITIALIZING ELECTION");
        initElection();
    }

    /* Parse configuration file with node IP addresses and ports. */
    public static HashMap<Integer, AddrPair> parseConfigFile(String fname) {
        HashMap<Integer, AddrPair> addrs = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fname))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] s = line.split("\\s", 3);
                AddrPair t = new AddrPair(s[1], Integer.parseInt(s[2]));
                addrs.put(Integer.parseInt(s[0]), t);
            }
        }
        catch (IOException e) {
            System.err.println(e);
        }
        return  addrs;
    }

    /* Initialize data structure to store neighboring nodes. */
    public void initializeNeighbors(HashMap<Integer, AddrPair> addrs) { neighbors = addrs; }

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.out.println("Arguments: <current node id> <configuration file>");
            System.exit(0);
        }

        int id = Integer.parseInt(args[0]);
        HashMap<Integer, AddrPair> temp = parseConfigFile(args[1]);
        Node n = new Node(temp.get(id).port, id);
        n.initializeNeighbors(temp);
        n.begin();
    }
}
