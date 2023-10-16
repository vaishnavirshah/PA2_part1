package client;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.SingleServer;
import server.MyDBSingleServer;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * This class should implement your DB client.
 */

public class MyDBClient extends Client {
    private NodeConfig<String> nodeConfig= null;
    private SingleServer customServer;
    
    private Map<String, Callback> requestCallbackMap = new HashMap<>();
    private int requestIdCounter = 1;

    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        this.nodeConfig = nodeConfig;
    }
    // public MyDBClient(SingleServer customServer) throws IOException {

    //     super();
    //     this.customServer = customServer;
    // }
    protected void handleResponse(byte[] bytes, NIOHeader header) {
        // expect echo reply by default here
        // try {
        //     System.out.println("Here in HR");
        //System.out.println(new String(bytes, SingleServer.DEFAULT_ENCODING));
        // } catch (UnsupportedEncodingException e) {
        //     e.printStackTrace();
        // }
        String responseString = new String(bytes);
       // System.out.println("Line 41 DBClient: "+responseString);
        int delimiterIndex = responseString.indexOf(':');

        if (delimiterIndex == -1) {
            System.out.println("*Received a response without a matching request");
            return;
        }

        String requestId = responseString.substring(0, delimiterIndex);
        String response = responseString.substring(delimiterIndex + 1);
        String requestToGet = "Request-"+requestId;
        //System.out.println("Line 51: "+ requestToGet + " resp: "+ response);
        // Get the callback associated with the request identifier

        Callback callback = requestCallbackMap.get(requestToGet);

        if (callback != null) {
            // Invoke the callback with the response
            callback.handleResponse(response.getBytes(), header);
           // System.out.println("*Operation completed");
        } else {
          //  System.out.println("in callback: Received a response without a matching request");
        }
    }
    public void callbackSend(InetSocketAddress isa, String request, Callback
            callback) throws IOException {

            String requestId = "Request-" + requestIdCounter;

            // Associate the request identifier with the callback
            //System.out.println("Line 69: putting requestId in hashmap: "+ requestId);
            requestCallbackMap.put(requestId, callback);
    
            // Send the request with the unique identifier
            String reqToSend = requestId + ":" + request;
           // System.out.print("Line 77: Request being sent: " + reqToSend);
            super.send(isa, reqToSend);
            requestIdCounter++;

        //throw new RuntimeException("Line77");
    }
    public static void main(String[] args) throws IOException {
        String req = "SELECT * FROM users WHERE lastname='Jones'";
        
        Callback callback = (bytes, header) -> {
            System.out.println("*Response from server: " + new String(bytes));
        };

        // Create an instance of MyDBClient and pass the custom server to the constructor.
        //new MyDBClient(customServer).send(MyDBSingleServer.getSocketAddress(args), req);//, callback);   
        NodeConfig<String> nc = null;   
        new MyDBClient(nc).callbackSend(MyDBSingleServer.getSocketAddress(args), req, callback);    
         
    }
}