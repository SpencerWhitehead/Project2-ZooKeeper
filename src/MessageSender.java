/**
 * Spencer Whitehead, whites5
 * Vipula Rawte, rawtev
 */

import java.io.*;
import java.net.Socket;

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
            System.err.println(e);
        }
    }
}
