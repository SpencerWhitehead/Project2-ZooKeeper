/**
 * Spencer Whitehead, whites5
 * Partha Sarathi Mukherjee, mukhep
 */

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;

/* Class for formatting and sending message between nodes. */
public class MessageSender {

    public MessageSender() {}

    /* Format message string. */
    public static String formatMsg(String command, int nodeID, String fname, String data){
        StringBuilder s = new StringBuilder();
        s.append(command);
        s.append("|");
        s.append(nodeID);
        s.append("|");
        s.append(fname);
        s.append("|");
        if (data != null){
            s.append(data);
        }
        return s.toString();
    }

    public static String formatMsg(ArrayList<String> contents) {
        StringBuilder s = new StringBuilder();
        int i;
        for(i=0; i<contents.size(); i++) {
            s.append(contents.get(i));
            s.append("|");
        }
        return s.toString();
    }

    public static String formatMsg(String[] contents) {
        StringBuilder s = new StringBuilder();
        int i;
        for(i=0; i<contents.length; i++) {
            s.append(contents[i]);
            s.append("|");
        }
        return s.toString();
    }

    public synchronized static void sendMsg(Socket sock, String msg) {
        try {
            PrintStream os = new PrintStream(sock.getOutputStream());
            os.println(msg);
//            os.close();
        }
        catch (IOException e) {
            System.err.println("INSIDE THE SEND MSG FUNCTION");
            System.err.println(e);
        }
    }

    /* Send message to other node. */
    public static void sendMsg(String receiverIP, int receiverPort, String msg){
        try {
            Socket socket = new Socket(receiverIP, receiverPort);
            PrintStream os = new PrintStream(socket.getOutputStream());
            os.println(msg);
            os.close();
            socket.close();
        }
        catch (IOException e) {
            System.err.println("INSIDE THE SEND MSG FUNCTION");
            System.err.println(e);
        }
    }
}
