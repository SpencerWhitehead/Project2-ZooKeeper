/**
 * Spencer Whitehead, whites5
 * Partha Sarathi Mukherjee, mukhep
 */

/*
Notes:
a) The number of entries in the configuration file must be exactly equal 
to the number of total server nodes 
b) The buffer till delivery on recv commit has not been implemented
c) The client connections are not stored yet, herefore, displaying on server on read
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
    private int epoch = 0;
    private int counter = 0;
    private String initSend;
    private Election elect;
    private HashMap<Integer, AddrPair> neighbors = new HashMap<>(); 
    // Map to store IP addresses and port numbers of neighbor nodes.
    
    private ConcurrentHashMap<Integer, Socket> connections = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Token> tokens = new ConcurrentHashMap<>(); 
    // Map to store token objects.
    
    private ConcurrentHashMap<String, Queue<String[]>> commands = new ConcurrentHashMap<>(); 
    // Map to  store what commands should be ran on each file.
    
    private ConcurrentHashMap<String,Integer> ackCount = new ConcurrentHashMap<>(); 
    //key: "<epoch>_<counter>" value: number of acks for this 
    
    private File historyFile = null;
    private ConcurrentSkipListSet<String> committedTransactions = new ConcurrentSkipListSet<>();
    public Node(int port, int ident) {
        this.portNum = port;
        this.ID = ident;
        StringBuilder s = new StringBuilder();
        s.append("UP|");
        s.append(ID);
        s.append("|");
        initSend = s.toString();
        createHistoryFile();
        elect = new Election();
    }

    /* Create history file. */
    private void createHistoryFile()
    {
        String fname = Integer.toString(ID)+"_hist_file";
        try
        {
            this.historyFile = new File(fname);
            boolean b = this.historyFile.createNewFile();
            if(!b)
            {
                System.out.println("History File already exists");
                System.out.println("Was there a crash recovery?");
            }
                
            
        }
        catch(IOException e)
        {
            System.out.println("Error! History file could not be created ");
        }
    }

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
        if (tokens.containsKey(fname)) 
        {
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

    private boolean toSend(int nodeID, int criteria) {
        if (criteria == 1) { return nodeID > ID; }
        else if (criteria == -1) { return nodeID < ID; }
        else if (criteria == 0) { return nodeID != ID; }
        else { return false;}
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
            System.out.println("ABOUT TO SEND " +msg+" MESSAGE TO "+Integer.toString(nodeID));
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
    
    private void sendToNodes(ArrayList<String> contents, int nodeID, int whichNodes) {
        String msg = MessageSender.formatMsg(contents);
        if(nodeID != 0 && whichNodes == -2 && connections.containsKey(nodeID)) {
            System.out.println("ABOUT TO SEND " +msg+" MESSAGE TO "+Integer.toString(nodeID));
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
    
    private void propose(String[] cts)
    {
        ++this.counter;
        ArrayList<String> li = new ArrayList<>();
        li.add("PRO");  
        for(int i = 0; i < cts.length; ++i)     li.add(cts[i]);
        li.add(Integer.toString(this.epoch));   li.add(Integer.toString(this.counter));   
        System.out.println("Sending following "+
        "proposal to all followers: " + MessageSender.formatMsg(li));
        sendToNodes(li,0,0);
        onRecvPropose(MessageSender.formatMsg(li),true);
    }
    
    private void onRecvPropose(String msg, boolean isSelf)
    {
        String[] ar = parseMsg(msg);
        if(!ar[0].equalsIgnoreCase("PRO"))
        {
            System.out.println("Error! Not a propose message!");
            System.exit(-1);
        }
        try
        {
            boolean b = isRepeated(msg);
            if(!b)
            {
                PrintWriter out = null;
                out = new PrintWriter(new BufferedWriter(new FileWriter(this.historyFile, true)));
                out.print(ar[ar.length-2]+"|"+ar[ar.length-1]+"|"); 
                for(int i = 1; i < ar.length-2;++i) 
                    out.print(ar[i]+"|");
                out.println();
                out.flush();
                out.close();               
            }
        }
        catch(IOException e)
        {
            System.out.println("Error in writing to disk. Message: "+msg);
            System.exit(-1);
        }
        if(!isSelf)
        {
            ar[0] = "ACK";
            sendToNodes(ar,leaderID,-2);
        }
    }
    
    private synchronized void onRecvAck(String msg)
    {
        String[] m = parseMsg(msg);
        String key = m[m.length-2]+"_"+m[m.length-1];
        Integer x = this.ackCount.get(key);
        if(x == null)  
            this.ackCount.put(key,1);
        else
            this.ackCount.put(key,x+1);
        
        // achieved majority; assume that leader itself has ack-ed, 
        //without sending an actual message                        
        System.out.println(ackCount.get(key)+"_"+neighbors.size());
        if(committedTransactions.contains(key)) return;
        if(ackCount.get(key)+1 > (this.neighbors.size()+1)/2) 
        {
            System.out.println("Hello again!");
            m[0] = "CMT";
            sendToNodes(m,0,0);
            onRecvCMT(MessageSender.formatMsg(m),true);
            committedTransactions.add(key);
        }
        //else do nothing
    }
    
    //***not buffering, immediately delivering***
    private void onRecvCMT(String msg, boolean isSelf)
    {
        //buffering part missing
        String[] ar = parseMsg(msg);
        if(ar.length<5 || (ar.length==5 && ar[0].equalsIgnoreCase("APP")))
        {
            System.out.println("Commit message is corrupt: "+msg);
            return;
        }
        if(ar[1].equalsIgnoreCase("NEW"))       createFile(ar[2]);
        else if(ar[1].equalsIgnoreCase("DEL"))  deleteFile(ar[2]);        
        else if(ar[1].equalsIgnoreCase("APP"))  appendFile(ar[2],ar[3]);        
        else    System.out.println("Commit message is corrupt: "+msg);
    }
    
    /* Parse incoming message. */
    private String[] parseMsg(String msg){ return msg.split("\\|"); }
    
    // check if transaction is repeated
    //algorithm: 
    //create: if file never created, false
    //else if number of creations less than equal to number of deletions of the file,false
    //true otherwise
    //delete: if file never deleted, false, otherwise, reverse of create
    //append: if last line appened to this file is the same, then true
    //else false
    private boolean isRepeated(String msg)
    {
        String[] ar = parseMsg(msg);
        if(ar.length<5)
        {
            System.out.println("Corrupted proposal message: too short "+msg);
            return true;
        }
        else
        {
            if(ar.length == 5 && ar[1].equalsIgnoreCase("APP")) 
            {
                System.out.println("Corrupted proposal message: "+
                "too short for appending "+msg);
                return true;                
            }
        }
        String[] tr = null;
        //storing the file name and the number of times
        //it has been created
        ConcurrentHashMap<String,Integer> creMap = new ConcurrentHashMap<>();
        
        //storing the file name and the number of times
        //it has been deleted
        ConcurrentHashMap<String,Integer> delMap = new ConcurrentHashMap<>();
        
        //storing the file name and the last line appended to it
        ConcurrentHashMap<String,String> appMap = new ConcurrentHashMap<>();
        Integer x = null, y=null; 
        String line = null;
        try 
        {
            FileReader fileRd = new FileReader(this.historyFile);
            BufferedReader bufRd = new BufferedReader(fileRd);
            while((line=bufRd.readLine())!=null)
            {
                if(line.trim().length()==0)
                    continue;
                tr = parseMsg(line);    
                if(tr.length<4)
                {
                    System.out.println("Error! Transaction entry is corrupted");
                    continue;
                }
                if(tr[2].equalsIgnoreCase("NEW"))
                {
                    x = creMap.get(tr[3]); //tr[3] is file name
                    if(x==null)     creMap.put(tr[3],1);
                    else            creMap.put(tr[3],x+1);
                }
                else if(tr[2].equalsIgnoreCase("DEL"))
                {
                    x = delMap.get(tr[3]); //tr[3] is file name
                    if(x==null)     delMap.put(tr[3],1);
                    else            delMap.put(tr[3],x+1);                    
                }
                else if(tr[2].equalsIgnoreCase("APP"))
                {
                    if(tr.length<5)
                    {
                        System.out.println("Error! Append Transaction entry is corrupted");
                        continue;
                    }
                    appMap.put(tr[3],tr[4]);
                }           
                else
                {
                    System.out.println("Corrupted entry in history file");
                    continue;
                }
            }
            x = creMap.get(ar[2]);      y = delMap.get(ar[2]);
            int x1=-1,y1=-1;
            if(ar[1].equalsIgnoreCase("NEW"))
            {
                if(x==null) return false;
                if(y==null) return true;
                x1 = x; y1 = y;
                if(x1 <= y1) return false;
                if(x1 > y1) return true;
            }
            
            else if(ar[1].equalsIgnoreCase("DEL"))
            {
                if(y==null) return false; //never been deleted
                if(x==null)
                {
                    System.out.println("Error!"
                    +"Corrupted history file! No create entry for file being deleted");
                    return true;
                }
                x1 = x; y1 = y;
                if(x1>=y1)  return false;//more creations than deletions
                return true;
            }
            else if(ar[1].equalsIgnoreCase("APP"))
            {
                String s = appMap.get(ar[2]);
                if(s==null) return false; // 1st statement being appended
                if(s.equals(ar[3]))  return true;
                return false;
            }
            else
            {
                System.out.println("Corrupted proposal message "+msg);
                return true;
            }
        } 
        catch(IOException e) 
        {
            System.out.println("Error in reading history file inside isRepeated function");
            System.out.println(e);
            return true;
        }
        return true;
    }
    
    private void initElection(){
        System.out.println("ATTEMPTING ELECTION");
        elect.holdElection();
        sendToNodes(new String[] {"ELE", Integer.toString(ID)}, 0, 1);
        try {
            Thread.sleep(1000);
            if(elect.getNumOkays() == 0) {
                System.out.println("No responses... Guess I'm the leader");
                leaderID = ID;
                sendToNodes(new String[] {"COR", Integer.toString(ID)}, 0, -1);
                elect.endElection();
                ++this.epoch;
                this.counter = 0;
            }
            else {
                Thread.sleep(1500);
                if(elect.recvdCoord()) {
                    leaderID = elect.getCoord();
                }
                else { initElection(); }
            }
        }
        catch (InterruptedException e) {
            System.err.println("Initiate election error:");
            System.err.println(e);
        }
    }

    private void onElectRecv(int nodeID) {
        System.out.println("SENDING OKAY MESSAGE");
        sendToNodes(new String[] {"OKA", Integer.toString(ID)}, nodeID, -2);
        if(!elect.ongoingElection()) { initElection(); }
    }

    private void onCoordRecv(int nodeID) {
        if(ID > nodeID && !elect.ongoingElection()) { initElection(); }
        else {
            try {
                leaderID = nodeID;
                elect.setCoord(leaderID);
                Thread.sleep(2600);
                System.out.println("NEW LEADER IS: " + Integer.toString(leaderID));
                elect.endElection();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class ElectHandler implements Runnable {
        private String[] msg;
        public ElectHandler(String[] m) { msg = m; }

        public void run() {
            switch (msg[0]) {
                case "ELE":
                    int n = Integer.parseInt(msg[1]);
                    Node.this.onElectRecv(n);
                    break;
                case "COR":
                    Node.this.onCoordRecv(Integer.parseInt(msg[1]));
                    break;
                case "OKA":
                    Node.this.elect.addOkay(Integer.parseInt(msg[1]));
                    break;
            }
        }
    }

    /*
    Message Formats:
    client to server: NEW|file name|
    server to leader: NEW|file name|; = <MSG>
                      ACK|<MSG>|epoch|counter|
    leader to server: PRO|<MSG>|epoch|counter|
                      CMT|<MSG>|epoch|counter|
    */
    /* Class to handle incoming messages. */
    private class ConnectHandler implements Runnable {
        private Socket socket = null; // Socket of incoming connection.
        private BufferedReader is = null; // Buffer to read incoming message.
        private PrintWriter os = null;
        private int connID = -1;
        public ConnectHandler(Socket sock) { socket = sock; }

        /* Parse and perform actions based on message. */
        private void handleMsg(String msg) 
        {
            String[] m = Node.this.parseMsg(msg);
            if(leaderID != ID && (m[0].equalsIgnoreCase("NEW") || m[0].equalsIgnoreCase("DEL") || m[0].equalsIgnoreCase("APP")))
                Node.this.sendToNodes(m,leaderID,-2);
            
            else
            {
                switch (m[0])
                {
                    case "NEW":
                    case "DEL":
                    case "APP":
                        Node.this.propose(m);
                        break;
                    /* If RED is keyword, then read file. */
                    case "RED":
                        System.out.println("\tReading file: "+m[1]);
                        Node.this.readFile(m[1]);
                        break;
                    case "PRO":
                        Node.this.onRecvPropose(msg,false);
                        break;
                    case "ACK":
                        Node.this.onRecvAck(msg);
                        break;
                    case "CMT":
                        Node.this.onRecvCMT(msg,false);
                        break;                    
                    case "ELE":
                        System.out.println("Received election message from: "+m[1]);
                        Thread electThread = new Thread(new ElectHandler(m));
                        electThread.start();
                        break;
                    case "COR":
                        System.out.println("Received coordinator message from: "+m[1]);
                        electThread = new Thread(new ElectHandler(m));
                        electThread.start();
                        break;
                    case "OKA":
                        System.out.println("Received OK message from: "+m[1]);
                        electThread = new Thread(new ElectHandler(m));
                        electThread.start();
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
            
        }

        /* Read in and handle message. */
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
//                e.printStackTrace();
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
