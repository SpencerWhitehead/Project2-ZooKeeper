/**
 * Spencer Whitehead, whites5
 * Partha Sarathi Mukherjee, mukhep
 */

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
*  Class to perform functionalities of a node in Raymond's algorithm.
*  Throughout this file and others, the words token and file are used
*  interchangeably. As in each file is a token and is represented by a
*  token object.
*/
public class Node {
    private int portNum;	// Port number on which node will be listening to accept connections
    private int ID;	        // ID of node
    private HashMap<Integer, AddrPair> neighbors = new HashMap<>(); // Map to store IP addresses and
                                                                    // port numbers of neighbor nodes.
    private ConcurrentHashMap<Integer, Socket> connections = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Token> tokens = new ConcurrentHashMap<>(); // Map to store token objects.
    private ConcurrentHashMap<String, Queue<String[]>> commands = new ConcurrentHashMap<>(); // Map to
                                                                                            // store what commands
                                                                                            // should be ran on each file.

    public Node(int port, int ident) {
        this.portNum = port;
        this.ID = ident;
    }

    /* Create file. */
    private void createFile(String fname, int nodeID) {
        if (!tokens.containsKey(fname)) {
            Token t = new Token(fname, nodeID);
            tokens.put(fname, t);
            System.out.println("\tCreated file: "+fname);
            StringBuilder s = new StringBuilder();
            s.append("\tNumber of tokens: ");
            s.append(tokens.size());
            System.out.println(s.toString());
            relayToNeighbors("NEW", fname, nodeID); // Notify neighboring nodes.
        }
        else {
            System.err.println("\tError: file already exists, "+fname);
        }
    }

    /* Delete file. */
    private void deleteFile(String fname, int nodeID) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.remove(fname);
            Queue q = commands.remove(fname);
            System.out.println("\tDeleted file: "+fname);
            StringBuilder s = new StringBuilder();
            s.append("\tNumber of tokens: ");
            s.append(tokens.size());
            System.out.println(s.toString());
            relayToNeighbors("DEL", fname, nodeID); // Notify neighboring nodes.
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

    /* Send request to node with token from Raymond's algorithm. */
    private void sendRequest(String fname) {
        if(tokens.containsKey(fname)) {
            Token t = tokens.get(fname);
            if (t.getHolder() != ID && !t.isReqQEmpty() && !t.getAsked()) {
                System.out.println("\tSending request for " + fname);
                String msg = MessageSender.formatMsg("REQ", ID, fname, null);
                MessageSender.sendMsg(neighbors.get(t.getHolder()).addr, neighbors.get(t.getHolder()).port, msg);
                t.setAsked(true);
                tokens.put(fname, t);
            }
        }
    }

    /* Send message to all neighbors. */
    private void relayToNeighbors(String command, String fname, int prevID){
        String msg = MessageSender.formatMsg(command, ID, fname, null);
        for(Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
            if (!entry.getKey().equals(prevID)) {
                StringBuilder s = new StringBuilder();
                s.append("\tNotifying node ");
                s.append(entry.getKey());
                System.out.println(s.toString());
                String addr = entry.getValue().addr;
                int port = entry.getValue().port;
                MessageSender.sendMsg(addr, port, msg);
            }
        }
    }

    /* Determine if incoming command is valid. */
    private boolean validateCommand(String command) {
        boolean valid = false;
        switch (command){
            /* If create is command, then it is valid. */
            case "create":
                valid = true;
                break;
            /* If delete is command, then it is valid. */
            case "delete":
                valid = true;
                break;
            /* If read is command, then it is valid. */
            case "read":
                valid = true;
                break;
            /* If append is command, then it is valid. */
            case "append":
                valid = true;
                break;
            default:
                break;
        }
        return valid;
    }

    /* Parse incoming command. */
    private String[] parseCommand(String com){
        return com.split("\\s",3);
    }

    /* Execute a given command on token. */
    private void runCommand(String command, String fname, String contents){
        switch (command){
            /* If create is command, then create file. */
            case "create":
                createFile(fname, ID);
                break;
            /* If delete is command, then delete file. */
            case "delete":
                deleteFile(fname, ID);
                break;
            /* If read is command, then read file. */
            case "read":
                readFile(fname);
                break;
            /* If append is command, then append to file. */
            case "append":
                if(contents != null) {
                    appendFile(fname, contents);
                }
                break;
            default:
                System.err.println("\tInvalid command: "+command);
                break;
        }
    }

    /* Class to handle incoming messages. */
    public class ConnectHandler implements Runnable {
        private Socket socket = null; // Socket of incoming connection.
        private BufferedReader is = null; // Buffer to read incoming message.
        private PrintWriter os = null;

        public ConnectHandler(Socket socket) {this.socket = socket;}

        /* Parse incoming message. */
        private String[] parseMsg(String msg){ return msg.split("\\|",4); }


        /* Parse and perform actions based on message. */
        private void handleMsg(String msg) {
            String[] m = parseMsg(msg);
            switch (m[0]){
                /* If NEW is keyword, then create file. */
                case "NEW":
                    if(!Node.this.tokens.containsKey(m[2])) {
                        System.out.println("\tCreating file: "+m[2]);
                        Node.this.createFile(m[2], Integer.parseInt(m[1]));
                    }
                    break;
                /* If DEL is keyword, then delete file. */
                case "DEL":
                    if(Node.this.tokens.containsKey(m[2])) {
                        System.out.println("\tDeleting file: "+m[2]);
                        Node.this.deleteFile(m[2], Integer.parseInt(m[1]));
                    }
                    break;
                /* If REQ is keyword, then request token. */
                case "REQ":
                    System.out.println("\tReceived request for file: "+m[2]);
                    break;
                /* If TOK is keyword, then handle token. */
                case "TOK":
                    System.out.println("\tReceived token: "+m[2]);
                    break;
                default:
                    System.err.println("\tInvalid message: "+msg);
                    break;
            }
        }

        /* Read in and handle message. */
        public void run() {
            try {
                is = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                os = new PrintWriter(socket.getOutputStream(), true);
                while (true) {
                    String msg = is.readLine();
                    if(msg == null) {
                        break;
                    }
                    System.out.println("\tReceived: " + msg);
//                    handleMsg(msg);
                    os.println("GOT IT!");
//                    Thread.sleep(4);
                }
                is.close();
                os.close();
                System.out.println("LOST CLIENT CONNECTION");
            }
//            catch (IOException|InterruptedException e){
            catch (IOException e){
                System.err.println(e);
            }
        }
    }

    /* Start server and accept connections. Each connection is handled in a thread. */
    public void begin() {
        try {
            for(Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
                if(entry.getKey() < ID) {
                    AddrPair loc = entry.getValue();
                    Socket sock = new Socket(loc.addr, loc.port);
                    Thread connThread = new Thread(new ConnectHandler(sock));
                    connThread.start();
                }
            }
            ServerSocket serverSocket = new ServerSocket(portNum);
            while(true) {
                Socket sock = serverSocket.accept();
                Thread connThread = new Thread(new ConnectHandler(sock));
                connThread.start();
            }
        }
        catch(IOException e){
            System.err.println(e);
        }
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

    /* Parse tree file and initialize data structure to store neighboring nodes. */
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