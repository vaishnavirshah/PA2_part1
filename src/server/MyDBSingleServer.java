package server;
import com.datastax.driver.core.*;


import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {
    Cluster cluster;
	Session session;
    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("demo");
        //session.execute("create table if not exists users (lastname text, age int, city text, email text, firstname text, PRIMARY KEY (lastname))");

		// Insert one record into the users table
		//session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");

    }
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // simple echo server
        try {
            String x = new String(bytes, SingleServer.DEFAULT_ENCODING);
           // System.out.println("line 36 of DBSS: "+x);

            String[] parts = x.split(":");
            String[] idParts = parts[0].split("-");

            // Extract the numeric part of the requestId
            //System.out.println("LINE 42  "+parts[0]);
            int requestId = 0;
            String requestContent;
            if(parts.length == 2){
                requestId = Integer.parseInt(idParts[1]);
                requestContent = parts[1];
            }else{
                requestId = 0;
                requestContent = parts[0];
            }
            

            //System.out.println("Before execution: " + requestId + " and " + requestContent);
            ResultSet results = session.execute(requestContent);
            // for (Row row : results) {
            //     System.out.format("%s %d\n", row.getString("firstname"),
            //             row.getInt("age"));
            // }
            log.log(Level.INFO, "line 50: Overrided function {0} received message from {1}", new Object[]
                    {this.clientMessenger.getListeningSocketAddress(), header.sndr});
            String resp = requestId+":"+"Executed the request";

            byte[] b = resp.getBytes();

            this.clientMessenger.send(header.sndr, b);
        } catch (IOException e) {
            System.out.println("Failed: Line 61 ");
            e.printStackTrace();
        }
    }
}

