/**
 * Spencer Whitehead, whites5
 * Partha Sarathi Mukherjee, mukhep
 */

/*
Notes:
a) The number of entries in the configuration file must be exactly equal 
to the number of total server nodes 
b) The buffer till delivery on recv commit has not been implemented
c) The client serverConnections are not stored yet, herefore, displaying on server on read
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
    private int portNum;    // Port number on which node will be listening to accept connections
    private int ID;         // ID of node
    private int leaderID = -1;
    private ZXID zxid = new ZXID(0,0);
    private String initSend;
    private Election elect;
    private HashMap<Integer, AddrPair> neighbors = new HashMap<>(); 
    // Map to store IP addresses and port numbers of neighbor nodes.
    
    private ConcurrentHashMap<Integer, Socket> serverConnections = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Socket> clientConnections = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Token> tokens = new ConcurrentHashMap<>(); 
    // Map to store token objects.
    
    private ConcurrentHashMap<String, Queue<String[]>> commands = new ConcurrentHashMap<>(); 
    // Map to  store what commands should be ran on each file.
    
    private ConcurrentHashMap<String,Integer> ackCount = new ConcurrentHashMap<>(); 
    //key: "<epoch>_<counter>" value: number of acks for this 
    
    private File historyFile = null;
    //private final File historyFile = new File(Integer.toString(ID)+"_hist_file");
    private ConcurrentSkipListSet<String> committedTransactions = new ConcurrentSkipListSet<>();
    
    private ConcurrentSkipListSet<String> queuedTransSet = new ConcurrentSkipListSet<>();
    private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    
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
    private void createHistoryFile() {
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

    private void execHistItem(String[] com) {
        switch (com[2]) {
            case "NEW":
                createFile(com[3]);
                break;
            case "DEL":
                deleteFile(com[3]);
                break;
            case "APP":
                appendFile(com[3], com[4]);
        }
    }

//    private void updateTokensFromHistory(ConcurrentLinkedQueue<String> comQ) {
//        while (comQ.size() > 0) {
//            String msg = comQ.poll();
//            String[] com = parseMsg(msg);
//            execHistItem(com);
//        }
//    }

    private void updateTokensFromHistory() {
        tokens.clear();
        synchronized (historyFile) {
            try (BufferedReader br = new BufferedReader(new FileReader(historyFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] com = parseMsg(line);
                    execHistItem(com);
                }
            } catch (IOException e) {
                System.err.println(e);
            }
        }
    }

//    private ZXID diffHistories(String[] history, String delimiter) {
//        try (BufferedReader br = new BufferedReader(new FileReader(historyFile))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//
//            }
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    private void mergeHistories(String[] history, boolean naive) {
        naive = true;
        ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>();
        synchronized (historyFile) {
            try {
                if (naive) {
                    PrintWriter writer = new PrintWriter(historyFile);
                    writer.print("");
                    int i;
                    for(i=0; i<history.length; i++) {
                        writer.println(buildHistEntry(parseMsg(history[i]), true));
                    }
                    writer.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized void matchLeaderHist(String[] history, boolean naive) {
        mergeHistories(history, naive);
        updateTokensFromHistory();
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

    /* Read specified file. */
    private String readFile(String fname) {
        StringBuilder st = new StringBuilder();
        if (tokens.containsKey(fname)) {
            Token t = tokens.get(fname);
            st.append("\tReading "+fname+":");
            st.append("\t\t"+tokens.get(fname).getContents());
        }
        else {
            st.append("ERR|Error: no such file, "+fname);
        }
        return st.toString();
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
        if(nodeID != 0 && whichNodes == -2 && serverConnections.containsKey(nodeID)) {
            System.out.println("ABOUT TO SEND " +msg+" MESSAGE TO "+Integer.toString(nodeID));
            MessageSender.sendMsg(serverConnections.get(nodeID), msg);
        }
        else {
            for (Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
                if (toSend(entry.getKey(), whichNodes) && serverConnections.containsKey(entry.getKey())) {
                    MessageSender.sendMsg(serverConnections.get(entry.getKey()), msg);
                }
            }
        }
    }
    
    private void sendToNodes(ArrayList<String> contents, int nodeID, int whichNodes) {
        String msg = MessageSender.formatMsg(contents);
        if(nodeID != 0 && whichNodes == -2 && serverConnections.containsKey(nodeID)) {
            System.out.println("ABOUT TO SEND " +msg+" MESSAGE TO "+Integer.toString(nodeID));
            MessageSender.sendMsg(serverConnections.get(nodeID), msg);
        }
        else {
            for (Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
                if (toSend(entry.getKey(), whichNodes) && serverConnections.containsKey(entry.getKey())) {
                    MessageSender.sendMsg(serverConnections.get(entry.getKey()), msg);
                }
            }
        }
    }

    private void sendToNodes(String msg, int nodeID, int whichNodes) {
        if(nodeID != 0 && whichNodes == -2 && serverConnections.containsKey(nodeID)) {
            System.out.println("ABOUT TO SEND " +msg+" MESSAGE TO "+Integer.toString(nodeID));
            MessageSender.sendMsg(serverConnections.get(nodeID), msg);
        }
        else {
            for (Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
                if (toSend(entry.getKey(), whichNodes) && serverConnections.containsKey(entry.getKey())) {
                    MessageSender.sendMsg(serverConnections.get(entry.getKey()), msg);
                }
            }
        }
    }

    private synchronized void propose(String[] cts) {
        ArrayList<String> li = new ArrayList<>();
        li.add("PRO");  
        for(int i = 0; i < cts.length; ++i)     li.add(cts[i]);
        li.add(Integer.toString(zxid.getEpoch()));   
        li.add(Integer.toString(zxid.getCounter()));
        zxid.updateCounter();
        System.out.println("Sending following "+
        "proposal to all followers: " + MessageSender.formatMsg(li));
        sendToNodes(li,0,0);
        onRecvPropose(MessageSender.formatMsg(li),true);
    }
    
    private void onRecvPropose(String msg, boolean isSelf) {
        String[] ar = parseMsg(msg);
        if(!ar[0].equalsIgnoreCase("PRO"))
        {
            System.out.println("Error! Not a propose message!");
            System.exit(-1);
        }
        updateHistory(ar, false);
        ar[0] = "CMT";
        boolean b = this.queuedTransSet.add(MessageSender.formatMsg(ar));   
        if(b) this.queue.add(MessageSender.formatMsg(ar));        
        if(!isSelf)
        {
            ar[0] = "ACK";
            sendToNodes(ar,leaderID,-2);
        }
    }
    
    private synchronized void onRecvAck(String msg) {
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
        if(ackCount.get(key)+1 > this.neighbors.size()/2) 
        {
            System.out.println("Hello again!");
            m[0] = "CMT";
            sendToNodes(m,0,0);
            onRecvCMT(MessageSender.formatMsg(m),true);
            committedTransactions.add(key);
        }
        //else do nothing
    }
    
    //***deliver items in buffer queue then deliver***
    private void onRecvCMT(String msg, boolean isSelf) {
        while(this.queue.size()>0) {
            String s = this.queue.poll();
            this.queuedTransSet.remove(s);
            String[] ar = parseMsg(s);
            if(ar[1].equalsIgnoreCase("NEW"))       createFile(ar[2]);
            else if(ar[1].equalsIgnoreCase("DEL"))  deleteFile(ar[2]);        
            else if(ar[1].equalsIgnoreCase("APP"))  appendFile(ar[2],ar[3]);        
            else    System.out.println("Commit message is corrupt: "+msg);
            int sID = Integer.parseInt(ar[ar.length-3]);
            if(this.ID == sID)
            {
                ar[0] = "SUC";
                Socket soc = this.clientConnections.get(ar[ar.length-4]);
                MessageSender.sendMsg(soc,MessageSender.formatMsg(ar));
            }
            if(s.equals(msg))   break;
        }
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
    private boolean isRepeated(String msg) {
        String[] ar = parseMsg(msg);
        if(ar.length<3)
        {
            System.out.println("Corrupted message: too short "+msg);
            return true;
        }
        else
        {
            if(ar.length == 3 && ar[0].equalsIgnoreCase("APP")) 
            {
                System.out.println("Corrupted message: "+
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
            x = creMap.get(ar[1]);      y = delMap.get(ar[1]);
            int x1=-1,y1=-1;
            if(ar[0].equalsIgnoreCase("NEW"))
            {
                if(x==null) return false;
                if(y==null) return true;
                x1 = x; y1 = y;
                if(x1 <= y1) return false;
                if(x1 > y1) return true;
            }
            
            else if(ar[0].equalsIgnoreCase("DEL"))
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
            else if(ar[0].equalsIgnoreCase("APP"))
            {
                String s = appMap.get(ar[1]);
                if(s==null) return false; // 1st statement being appended
                if(s.equals(ar[2]))  return true;
                return false;
            }
            else
            {
                System.out.println("Corrupted message "+msg);
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
    
    private boolean isInvalid(String msg,List<String> li) {
        String[] ar = parseMsg(msg);
        if(!ar[0].equalsIgnoreCase("RED") && isRepeated(msg))
        {
            li.add("ERR|Error: Transaction already present in history file: "+msg);
            return true;
        }
        if((ar[0].equalsIgnoreCase("APP") ||(ar[0].equalsIgnoreCase("DEL"))
        || ar[0].equalsIgnoreCase("RED")) && !this.tokens.containsKey(ar[1])) 
        {
            li.add("ERR|"+
            "Error: File does not exist "+msg);
            return true;                
        }
        if(ar[0].equalsIgnoreCase("NEW")&& this.tokens.containsKey(ar[1])) 
        {
            li.add("ERR|"+
            "Error: File already exists "+msg);            
            return true;
        }
        return false;
    }
    
    private synchronized String buildHistEntry(String[] m, boolean preformatted) {
        StringBuilder s = new StringBuilder();
        if(!preformatted) {
            s.append(m[m.length - 2]);
            s.append("|");
            s.append(m[m.length - 1]);
            s.append("|");
            int i;
            for (i = 1; i < m.length - 2; i++) {
                s.append(m[i]);
                s.append("|");
            }
        }
        else {
            int i;
            for (i = 0; i < m.length; i++) {
                s.append(m[i]);
                s.append("|");
            }
        }
        return s.toString();
    }

    private synchronized void updateHistory(String[] msg, boolean preformatted) {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(this.historyFile, true));
            bw.write(buildHistEntry(msg, preformatted));
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

    private synchronized String formHistoryMessage() {
        String line = null;
        StringBuilder st = new StringBuilder("HIS_");
        try 
        {
            FileReader fileRd = new FileReader(this.historyFile);
            BufferedReader bufRd = new BufferedReader(fileRd);
            while((line=bufRd.readLine())!=null)
            {
                if(line.trim().length()==0)
                    continue;
                st.append(line+"_");                
            }
        } 
        catch(Exception e) 
        {
            System.err.println("Error in reading history file");
            return null;
        }
        return st.toString();
    }
    
    private synchronized void sendHistoryMsg(int nodeID) {
        //nodeID zero indicates send to all nodes
        if(nodeID==0)   sendToNodes(formHistoryMessage(),nodeID,0);
        else    sendToNodes(formHistoryMessage(),nodeID,-2);
        
    }

    private String[] createLeaderElectMsg(String com) {
        return new String[] {com, Integer.toString(zxid.getEpoch()), 
        Integer.toString(zxid.getCounter()), Integer.toString(ID)};
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

    /*
    Message Formats:
    client to server: NEW|file name|client_name|serverID
    server to leader: NEW|file name|client_name|serverID; = <MSG>
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

        private void handleHistoryMsg(String msg) {
            String[] ar = msg.split("_");
            String[] ar1 = new String[ar.length-1];
            for(int i = 1;i<ar.length;++i) { ar1[i-1] = ar[i]; }
            Node.this.mergeHistories(ar1, true);
            Node.this.updateTokensFromHistory();
            
        }

        /* Parse and perform actions based on message. */
        private void handleMsg(String msg) {
            if(msg.substring(0,4).equals("HIS_")) {
                handleHistoryMsg(msg);
                return;
            }

            String[] m = Node.this.parseMsg(msg);
            List<String> li = new ArrayList<>();
            if(Node.this.leaderID != Node.this.ID && (m[0].equalsIgnoreCase("NEW") || 
            m[0].equalsIgnoreCase("DEL") || m[0].equalsIgnoreCase("APP")))
            {
                
                if(leaderID==-1 || 
                serverConnections.size() +1 <= neighbors.size()/2)
                {
                    MessageSender.sendMsg(socket,
                    "ERR|Error: Sorry Cannot process any commands right now. "+
                    "Please try again later");
                }
                    
                else    Node.this.sendToNodes(m,leaderID,-2);
            }
            else
            {
                switch (m[0])
                {
                    case "NEW":
                    case "DEL":
                    case "APP":
                        if(Node.this.serverConnections.size() +1 <= Node.this.neighbors.size()/2)
                        {
                            MessageSender.sendMsg(socket,
                            "ERR|Error: Sorry Cannot process any commands right now. "+
                            "Please try again later");
                        }                        
                        else
                        {
                            li.clear();
                            boolean b = Node.this.isInvalid(msg,li);
                            if(b)   MessageSender.sendMsg(socket,li.get(0)+"|"+m[m.length-2]);
                            else Node.this.propose(m);                            
                        }
                        break;
                    /* If RED is keyword, then read file. */
                    case "RED":
                        String ans = Node.this.readFile(m[1]);
                        MessageSender.sendMsg(socket,ans);                            
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
                    case "ERR":
                        Socket s = Node.this.clientConnections.get(m[m.length-1]);
                        MessageSender.sendMsg(s,msg);
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
                        if(!Node.this.serverConnections.containsKey(connID)) {
                            Node.this.serverConnections.put(connID, socket);
                            System.out.println("Added NodeID "+connID+" to serverConnections");
                            if(Node.this.leaderID == Node.this.ID)  {sendHistoryMsg(connID);}
                        }
                        break;
                    case "CLI":
                        if(!Node.this.clientConnections.containsKey(m[1])) {
                            Node.this.clientConnections.put(m[1], socket);
                            System.out.println("Added Client name "+m[1]+" to clientConnections");
                        }
                        break;
                    default:
                        System.err.println("\tInvalid message: "+msg);
                        break;
                }                
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
                    Node.this.serverConnections.remove(connID);
                    System.out.println(Node.this.serverConnections.size());
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

    /* Start server and accept serverConnections. Each connection is handled in a thread. */
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
                if (!serverConnections.containsKey(entry.getKey())) {
                    try {
                        AddrPair loc = entry.getValue();
                        Socket sock = new Socket(loc.addr, loc.port);
                        serverConnections.put(entry.getKey(),sock);
                        Thread connThread = new Thread(new ConnectHandler(serverConnections.get(entry.getKey())));
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
