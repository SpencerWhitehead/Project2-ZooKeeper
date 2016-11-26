import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;
import java.util.concurrent.*;

public class ClientThread implements Runnable
{
    private int serverPort;
    private String serverIP;
    private ClientZookeeper clZobj;
    
    ClientThread(String host, int port,ClientZookeeper clZobj)    
    {
        this.serverIP = host;   this.serverPort = port;
        this.clZobj = clZobj;
    }
    
    public void run()
    {
        try 
        {
            //System.out.println(1);           
            Socket s = new Socket(this.serverIP,this.serverPort);
            clZobj.soc = s;
            BufferedOutputStream bos = new BufferedOutputStream(s.
              getOutputStream());

            BufferedReader input =
                new BufferedReader(new InputStreamReader(s.getInputStream()));
            while(true)
            {
                String line = input.readLine();
                if(line == null)    break;
                if(line.length()<2) continue;
                char c1 = line.charAt(0);
                char c2 = line.charAt(1);
                if(c1=='U'&&c2=='P') continue;
                System.out.println(line);
            }
            System.out.println("Server has shut down");
            System.exit(-1);
        }             

        catch(Exception e) 
        {
            
        }
        
    }
}

