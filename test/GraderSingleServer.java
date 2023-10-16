import client.Client;
import client.MyDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.DefaultTest;
import org.junit.*;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import server.MyDBSingleServer;
import server.ReplicatedServer;
import server.SingleServer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GraderSingleServer extends DefaultTest {

    protected static final String DEFAULT_KEYSPACE = "demo";
    protected static final InetSocketAddress DEFAULT_SADDR = new InetSocketAddress
            ("localhost", 1999);
    protected static final InetSocketAddress DEFAULT_DB_ADDR = new
            InetSocketAddress("localhost", 9042);
    protected static final int NUM_SERVERS = 3;
    protected static final int SLEEP = 1000;
    protected static final String CONFIG_FILE = System.getProperty("config")
            != null ? System.getProperty("config") :
            "conf/servers.properties";

    protected static Client client = null;
    protected static SingleServer singleServer = null;
    protected static SingleServer[] replicatedServers = null;
    protected static Map<String, InetSocketAddress> serverMap = null;
    protected static String[] servers = null;
    protected static Cluster cluster;
    protected static Session session = (cluster = Cluster.builder().addContactPoint
            (DEFAULT_SADDR
                    .getHostName()).build()).connect(DEFAULT_KEYSPACE);

    protected final static String DEFAULT_TABLE_NAME = "grade";

    protected static NodeConfig<String> nodeConfigServer;

    protected static NodeConfig<String> nodeConfigClient;
    protected static final boolean GRADING_MODE = true;

    protected static final int NUM_REQS = 100;

    protected static final int SLEEP_RATIO = 10;

    @BeforeClass
    public static void setup() throws IOException {
        // setup single server
        singleServer = GRADING_MODE ? new MyDBSingleServer(DEFAULT_SADDR,
                DEFAULT_DB_ADDR, DEFAULT_KEYSPACE) :
                (SingleServer) getInstance(getConstructor("server" +
                                ".AVDBSingleServer",
                        InetSocketAddress.class, InetSocketAddress.class,
                        String.class), DEFAULT_SADDR, DEFAULT_DB_ADDR,
                        DEFAULT_KEYSPACE);
        nodeConfigServer = NodeConfigUtils.getNodeConfigFromFile
                (CONFIG_FILE, ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                        .SERVER_PORT_OFFSET);

        nodeConfigClient = NodeConfigUtils.getNodeConfigFromFile
                (CONFIG_FILE, ReplicatedServer.SERVER_PREFIX);
        // setup client
        client = GRADING_MODE ? new MyDBClient(nodeConfigClient) :
                (Client) getInstance(getConstructor("client.AVDBClient",
                        NodeConfig.class),
                        nodeConfigClient);

    }

    /**
     * This test sends a simple default DB command expected to always succeed
     * because we are not checking that the server does anything useful.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test01_DefaultAsync() throws IOException,
            InterruptedException {
        client.send(DEFAULT_SADDR, "select table_name from system_schema" +
                ".tables");
    }

    /**
     * Tests that a table is indeed being created successfully. This test
     * checks that the server processes the received command correctly by
     * relaying to the DB.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test02_Single_CreateTable_Async() throws IOException,
            InterruptedException {
        dropTableIfExists();
        System.out.println("here");
        testCreateTable(true, true);
    }


    /**
     * This test is similar to the test above and verifies that the server
     * correctly processes commands to insert records. If the server passes
     * the previous test, it should also pass this test, i.e., no new logic
     * is needed for this test.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test03_InsertRecords_Async() throws IOException, InterruptedException {
        int numInserts = 10;
        clearTableRecords();
        for (int i = 0; i < numInserts; i++) {
            send("insert into " + TABLE + " (ssn, firstname, lastname) " +
                    "values (" + (int) (Math.random() * Integer.MAX_VALUE) + ", '" +
                    "John" + i + "', '" + "Smith" + i + "')", true);
        }
        Thread.sleep(SLEEP);
        verifyInsertedNumRecordsExist(TABLE, DEFAULT_KEYSPACE, numInserts);
    }

    /**
     *
     * Same as above for deleting records. No new logic needed.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test04_DeleteRecords_Async() throws IOException, InterruptedException {
        send("truncate users", true);
        Thread.sleep(SLEEP);
        ResultSet resultSet = session.execute(getTableCountCmd(TABLE,DEFAULT_KEYSPACE));
        Assert.assertTrue(!resultSet.isExhausted());
        Assert.assertEquals(0, resultSet.one().getLong(0));
    }

    /**
     * This test checks if the client implements support for callbacks
     * correctly. It first deletes the table and then issues a command to
     * create the table along with a callback function expected to be invoked
     * by the client when it gets a confirmation of the command's successful
     * execution back from the server.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test05_CreateTable_Sync() throws IOException, InterruptedException {
        session.execute(getDropTableCmd(TABLE, DEFAULT_KEYSPACE));
        testCreateTable(true, false);
    }




    // The cleanup tests below will always succceed.


    protected void testCreateTable(boolean single, boolean sleep) throws
            IOException, InterruptedException {
        if (sleep) testCreateTableSleep(single);
        else testCreateTableBlocking(single);
        verifyTableExists(TABLE, DEFAULT_KEYSPACE, true);

    }

	/**
	 * This method will directly contact the DB and drop the table.
     * @throws IOException
     */
    protected void dropTableIfExists() throws IOException {
        session.execute(getDropTableCmd(TABLE, DEFAULT_KEYSPACE));
    }

    protected void clearTableRecords() throws IOException {
        session.execute(getTruncateTableCmd(TABLE, DEFAULT_KEYSPACE));
    }

    protected void verifyTableExists(String table, String keyspace, boolean exists) {
        ResultSet resultSet = session.execute("select table_name from " +
                "system_schema.tables where keyspace_name='" + keyspace + "'");
        Assert.assertTrue(!resultSet.isExhausted());
        boolean match = false;
        for (Row row : resultSet)
            match = match || row.getString("table_name").equals(table);
        if (exists)
            Assert.assertTrue(match);
        else
            Assert.assertFalse(match);
    }

    protected void verifyInsertedNumRecordsExist(String table, String keyspace,
                                               int numRequests) {
        ResultSet resultSet = session.execute("select count(*) from " +
                 keyspace + "." + table);
        Assert.assertTrue(!resultSet.isExhausted());
        boolean match = false;
        match = match || (resultSet.one().getLong(0) == numRequests);
        Assert.assertTrue(match);
    }

    protected void verifyOrderConsistent(String table, int key) {
        String[] results = new String[servers.length];
        int i = 0;
        boolean nonEmpty = false;
        for (String node : servers) {
            ResultSet result = session.execute(readResultFromTableCmd(key, DEFAULT_TABLE_NAME, node));
            results[i] = "";
            for (Row row : result) {
                results[i] += row;
                nonEmpty = true;
            }
            i++;
        }
        i = 0;
        boolean match = true;
        for (String result : results) {
            if (!results[0].equals(result))
                match = false;
        }
        Assert.assertTrue(nonEmpty && match);
        for(i=0; i<results.length; i++)
            System.out.println(i+":"+results[i]);

    }

    protected void testCreateTableSleep(boolean single) throws
            InterruptedException, IOException {
        send(getDropTableCmd(TABLE, DEFAULT_KEYSPACE), single);
        Thread.sleep(SLEEP);
        send(getCreateTableCmd(TABLE, DEFAULT_KEYSPACE), single);
        Thread.sleep(SLEEP);
    }


    ConcurrentHashMap<Long, String> outstanding = new ConcurrentHashMap<Long, String>();

    protected void testCreateTableBlocking(boolean single) throws
            InterruptedException, IOException {
        waitResponse(callbackSend(DEFAULT_SADDR, getDropTableCmd(TABLE, DEFAULT_KEYSPACE)));
        waitResponse(callbackSend(DEFAULT_SADDR, getCreateTableCmd(TABLE, DEFAULT_KEYSPACE)));
    }

    protected Long callbackSend(InetSocketAddress isa, String request) throws
            IOException {
        Long id = enqueueRequest(request);
        client.callbackSend(isa, request, new WaitCallback(id));
        return id;
    }

    protected class WaitCallback implements Client.Callback {
        Long monitor; // both id and monitor

        WaitCallback(Long monitor) {
            this.monitor = monitor;
        }

        @Override
        public void handleResponse(byte[] bytes, NIOHeader header) {
            synchronized (this.monitor) {
                outstanding.remove(monitor);
                this.monitor.notify();
            }
        }
    }

    protected long reqnum = 0;

    protected long enqueue() {
        synchronized (outstanding) {
            return reqnum++;
        }
    }

    protected long enqueueRequest(String request) {
        long id = enqueue();
        outstanding.put(id, request);
        return id;
    }

    protected void waitResponse(Long id) {
        synchronized (id) {
            while (outstanding.containsKey(id))
                try {
                    id.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }


    protected static final void send(String cmd, boolean single) throws
            IOException {
        client.send(single ? DEFAULT_SADDR :
                        serverMap.get(servers[(int) (Math.random() * servers.length)]),
                cmd);
    }

    protected static final String TABLE = "users";

    protected static String getCreateTableCmd(String table, String keyspace) {
        return "create table if not exists " + keyspace + "." + table + " (age int, firstname " +
                "text, lastname text, ssn int, address text, hash bigint, " +
                "primary key (ssn))";
    }

    protected static String getTableCountCmd(String table, String keyspace) {
        return "select count(*) from " + keyspace + "." + table;
    }

    protected static String getDropTableCmd(String table, String keyspace) {
        return "drop table if exists " + keyspace + "." + table;
    }

    protected static String getTruncateTableCmd(String table, String keyspace) {
        return "truncate table " + keyspace + "." + table;
    }

    protected static String getCreateTableWithList(String table, String keyspace) {
        return "create table if not exists " + keyspace + "." + table + " (id int, events list<int>, primary key (id));";
    }

    protected static String insertRecordIntoTableCmd(int key, String table) {
        return "insert into " + table + " (id, events) values (" + key + ", []);";
    }

    protected static String updateRecordOfTableCmd(int key, String table) {
        return "update " + table + " SET events=events+[" + incrSeq() + "] where id=" + key + ";";
    }

    // This is only used to fetch the result from the table by session directly connected to cassandra
    protected static String readResultFromTableCmd(int key, String table, String keyspace) {
        return "select events from " + keyspace + "." + table + " where id=" + key + ";";
    }

    protected static long sequencer = 0;

    protected synchronized static long incrSeq() {
        return sequencer++;
    }

    @AfterClass
    public static void teardown() {
        // clean up default table if left
        session.execute(getDropTableCmd(DEFAULT_TABLE_NAME, DEFAULT_KEYSPACE));
        if (client != null) client.close();
        if (singleServer != null) singleServer.close();
        session.close();
        cluster.close();
    }

    protected static Object getInstance(Constructor<?> constructor,
                                      Object... args) {
        try {
            return constructor.newInstance(args);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected static Constructor<?> getConstructor(String clazz, Class<?>... types) {
        try {
            Class<?> instance = Class.forName(clazz);
            return instance.getConstructor(types);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        Result result = JUnitCore.runClasses(GraderSingleServer.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
    }

}
