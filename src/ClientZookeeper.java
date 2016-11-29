import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;
import java.util.concurrent.*;

public class ClientZookeeper
{
    public Socket soc;
    public int serverID;
    public String name;
    
    public ClientZookeeper(int serverID,String name)
    {
        this.serverID = serverID;
        this.soc = null;
        this.name = name;
    }
    
    /* Parse configuration file with node IP addresses and ports. */
    public HashMap<Integer, AddrPair> parseConfigFile(String fname) 
    {
        HashMap<Integer, AddrPair> addrs = new HashMap<>();
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(fname));
            String line;
            while (true)
            {
                line = br.readLine();
                if(line==null)  break;
                String[] s = line.split("\\s", 3);
                AddrPair t = new AddrPair(s[1], Integer.parseInt(s[2]));
                addrs.put(Integer.parseInt(s[0]), t);
            }
        }
        catch (Exception e) 
        {
            e.printStackTrace();
        }
        return  addrs;
    }    
    
    public void inputCommandProcessing(String line)
    {
        try 
        {
            String[] tokens = line.split(" ",3);
            if(tokens[0].equalsIgnoreCase("exit"))
            {
                this.soc.close();   
                System.out.println("Byeee!!!");
                System.exit(-1);
            }
            
            StringBuilder st = new StringBuilder();
            if(tokens[0].equalsIgnoreCase("create"))    st.append("NEW|");
            if(tokens[0].equalsIgnoreCase("read"))    st.append("RED|");
            if(tokens[0].equalsIgnoreCase("append"))    st.append("APP|");
            if(tokens[0].equalsIgnoreCase("delete"))    st.append("DEL|");
            
            st.append(tokens[1]+"|");
            if(tokens.length > 2)   st.append(tokens[2]+"|");
            st.append(this.name+"|");
            st.append(this.serverID+"|");
            MessageSender.sendMsg(this.soc,st.toString());
        } 
        catch(Exception e) 
        {
            e.printStackTrace();            
        }        
    }
    

    
    public static void main(String[] args)
    {
        try 
        {
            if(args.length < 3)
            {
                System.out.println("Error. Please provide"+ 
                " the following command line arguments");
                System.out.println("1. Server Node ID");
                System.out.println("2. Configuration File Name");
                System.out.println("3. Client Name ");
                System.exit(-1);
            } 
            
            int id = Integer.parseInt(args[0]);
            String name = args[2];
            ClientZookeeper clZobj = new ClientZookeeper(id,name);
            
            HashMap<Integer, AddrPair> mapAddr = clZobj.parseConfigFile(args[1]);
            AddrPair adr = mapAddr.get(id);
            ClientThread cObj = new ClientThread(adr.addr,adr.port,clZobj);                
            Thread tObj = new Thread(cObj);
            tObj.start();
            Scanner scan = new Scanner(System.in);
            while(true)
            {
                System.out.println("Enter Command for Client connected to Server with ID "+id);
                String line = scan.nextLine();
                clZobj.inputCommandProcessing(line);
            }
        } 
        catch(Exception e) 
        {
            e.printStackTrace();
        }        
    }
}
