package com.facebook.LinkBench;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.TarantoolClientOps;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;

public class LinkStoreTarantool extends GraphStore {

    public static final int DEFAULT_BULKINSERT_SIZE = 1024;

    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_USER = "user";
    public static final String CONFIG_PASSWORD = "password";
    public static final String CONFIG_WRITE_SYNC = "write_options_sync";
    public static final String CONFIG_WRITE_DISABLE_WAL =
            "write_options_disableWAL";
    private Level debuglevel;


    private static String host;
    private static String user;
    private static String pwd;
    private static String port;


    // Mapping java methods to lua embedded functions
    private static final String METHOD_ADD_LINK = "insert_link";
    private static final String METHOD_ADD_BULK_LINKS = "insert_links";
    private static final String METHOD_GET_LINK = "get_link";
    private static final String METHOD_MULTI_GET_LINK = "multi_get_link";
    private static final String METHOD_DELETE_LINK = "delete_link";
    private static final String METHOD_GET_LINK_LIST = "get_link_list";
    private static final String METHOD_GET_LINK_LIST_TIME = "get_link_list_time_bound";
    private static final String METHOD_COUNT_LINKS = "count_links";
    private static final String METHOD_ADD_COUNTS = "add_counts";
    private static final String METHOD_ADD_BULK_NODES = "add_bulk_nodes";
    private static final String METHOD_GET_NODE = "get_node";
    private static final String METHOD_UPDATE_NODE = "update_node";
    private static final String METHOD_DELETE_NODE = "delete_node";


    int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;

    private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);


    private static TarantoolClientImpl client;
    private TarantoolClientOps<Integer, Object, Object, List> syncOps;

    //private  TarantoolSchema schema;

    /*
    * TODO: Correct dumb way to process results (checking whether response is empty)
    * */

    /*
    * TODO: Refactor getNodeList with time order inside database.
    * */

    /*
    * TODO: Rewrite constants sucah as index, space id more nicely
    * */

    @Override
    public void initialize(Properties p, Phase currentPhase, int threadId) throws IOException, IOException {

        host = ConfigUtil.getPropertyRequired(p, CONFIG_HOST);
        user = ConfigUtil.getPropertyRequired(p, CONFIG_USER);
        pwd = ConfigUtil.getPropertyRequired(p, CONFIG_PASSWORD);
        port = p.getProperty(CONFIG_PORT);

        if (port == null || port.equals("")) port = "3306"; //use default port
        debuglevel = ConfigUtil.getDebugLevel(p);

        // connect
        try {
            openConnection();
        } catch (Exception e) {
            logger.error("error connecting to database:", e);
            throw e;
        }
        //schema = conn.schema(new TarantoolSchema());
    }

    private static class Singleton {
        private static final Singleton singleton = new Singleton();

        private Singleton(){
            TarantoolClientConfig config = new TarantoolClientConfig();
            config.username = user;
            config.password = pwd;

            SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
                @Override
                public SocketChannel get(int retryNumber, Throwable lastError) {
                    if (lastError != null) {
                        lastError.printStackTrace(System.out);
                    }
                    try {
                        return SocketChannel.open(new InetSocketAddress(host, Integer.parseInt(port)));
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            };
            tarantoolClient = new TarantoolClientImpl(socketChannelProvider, config) {
                @Override
                public void configureThreads(String threadName){
                    super.configureThreads(threadName);
                    reader.setDaemon(true);
                    writer.setDaemon(true);
                }

            };

        }

        public static Singleton getInstance(){
            return singleton;
        }

        private TarantoolClientImpl tarantoolClient;

    }

    private void openConnection() throws IOException{

            Singleton singleton = Singleton.getInstance();

            client = singleton.tarantoolClient;

            syncOps = client.syncOps();

    }


    @Override
    public void close() {
        try {
            if (client.isAlive()) {
                //client.close();
            }
        } catch (Exception e){
            logger.error("Error while closing Tarantool connection: ", e);
        }
    }

    @Override
    public void clearErrors(int threadID) {
        logger.info("Failed in threadID " + threadID);

        try {
            logger.info("closing connection");
            close();
                   } catch (Throwable e) {
            e.printStackTrace();
            logger.error("Error in closing!" + e);
            return;
        }
    }

private static class Converter {
    public static List LinkToList(Link l) {
        List list = new ArrayList();

        list.add(l.id1);
        list.add(l.id2);
        list.add(l.link_type);
        list.add(l.visibility > 0);
        list.add(l.data);
        list.add(l.time);
        list.add(l.version);

        return list;
    }

    public static List NodeToList(Node n) {
        List list = new ArrayList();

        //no need in id here
        list.add(n.type);
        list.add(n.version);
        list.add(n.time);
        list.add(n.data);

        return list;
    }

    public static Object CountToList(LinkCount count) {
        List list = new ArrayList();

        list.add(count.id1);
        list.add(count.link_type);
        list.add(count.count);
        list.add(count.version);
        list.add(count.time);

        return list;
    }

    public static Link ListToLink(List l) {
        return new Link(((Number)l.get(0)).longValue(), ((Number)l.get(2)).longValue(), ((Number)l.get(1)).longValue(),
                (byte)((Boolean) l.get(3) ? 1 : 0),
                ((String)l.get(4)).getBytes(), ((Number)l.get(6)).intValue(), ((Number)l.get(5)).longValue());
    }


    public static Node ListToNode(List l) {
        return new Node(((Number)l.get(0)).longValue(), ((Number)l.get(1)).intValue(),
                ((Number)l.get(2)).longValue(), ((Number)l.get(3)).intValue(), ((String)l.get(4)).getBytes());
    }
}


    @Override
    public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {

        try{
            return addLinkImpl(a);
        } catch (Exception ex){
            logger.error("addLink failed! " + ex);
            throw ex;
        }
    }

    private boolean addLinkImpl(Link l) throws Exception{
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("addLink " + l.id1 +
                    "." + l.id2 +
                    "." + l.link_type);
        }
        syncOps.call(METHOD_ADD_LINK, Converter.LinkToList(l));
        //Implementation of return value is optional
        //return (boolean)((List)(res.get(0))).get(0);
        return true;
    }

    public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
            throws Exception {
        try {
            addBulkLinksImpl(links);
        } catch (Exception ex){
            logger.error("addBulkLinks failed! " + ex);
            throw ex;
        }
    }

    private void addBulkLinksImpl(List<Link> links) throws Exception{
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("addBulkLinks: " + links.size() + " links");
        }
        List objects = new ArrayList(links.size());
        for (Link link: links){
            objects.add(Converter.LinkToList(link));
        }
        syncOps.call(METHOD_ADD_BULK_LINKS, objects);
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        try {
            return deleteLinkImpl(id1, link_type, id2, expunge);
        } catch (Exception ex) {
            logger.error("deletelink failed! " + ex);
            throw ex;
        }
    }

    private boolean deleteLinkImpl(long id1, long link_type,
                                   long id2,  boolean expunge) throws Exception{
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("deleteLink " + id1 +
                    "." + id2 +
                    "." + link_type);
        }
        syncOps.call(METHOD_DELETE_LINK, Arrays.asList(id1, id2, link_type), expunge);
        return true;
    }

    @Override
    public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("updateLink " + a.id1 +
                    "." + a.id2 +
                    "." + a.link_type);
        }
        addLinkImpl(a);
        return true;
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        try {
            return getLinkImpl(id1, link_type, id2);
        } catch (Exception ex) {
            logger.error("getLink failed! " + ex);
            throw ex;
        }
    }

    private Link getLinkImpl(long id1, long link_type, long id2)
            throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("getLink " + id1 +
                    "." + id2 +
                    "." + link_type);
        }
        //List res =  conn.select(schema.links.id, schema.links.primary, Arrays.asList(id1, id2, link_type), 0, 1, 0);
        List res =  syncOps.call(METHOD_GET_LINK, Arrays.asList(id1, id2, link_type));

        if (((List)res.get(0)).isEmpty() || ((List)res.get(0)).get(0) == null)
            return null;
        return Converter.ListToLink((List)res.get(0));
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type,
                                long[] id2s) throws Exception {
        try {
            return multigetLinksImpl(id1, link_type, id2s);
        } catch (Exception ex) {
            logger.error("multigetlinks failed! " + ex);
            throw ex;
        }
    }

    private Link[] multigetLinksImpl(long id1, long link_type,
                                     long[] id2s) throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("multigetLink " + id1 +
                    "." + id2s.length +
                    "." + link_type);
        }

        List<Long> lid2 = new ArrayList<Long>();
        for (long l: id2s) {
            lid2.add(l);
        }

        List res = syncOps.call(METHOD_MULTI_GET_LINK, id1, link_type, lid2);

        if (((List)res.get(0)).isEmpty())
            return new Link[0];

        List<Link> links = new ArrayList<>();
        for (Object o: res){
            if (o != null && ((List) o).get(0) != null)
                try {
                    links.add(Converter.ListToLink((List) o));
                }catch (Exception e){
                    logger.info("got object problem to parse" + o);
                }
            }
        Link[] result = new Link[links.size()];
        return links.toArray(result);
    }


    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        try {
            return getLinkListImpl(id1, link_type);
        } catch (Exception ex) {
            logger.error("getLinkList failed! " + ex);
            throw ex;
        }
    }

    private Link[] getLinkListImpl(long id1, long link_type)
            throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("getLinkList " + id1 +
                    "." + link_type);
        }
        List res =  syncOps.call(METHOD_GET_LINK_LIST, id1, link_type);

        //List res = conn.select(schema.links.id, schema.links.id_type_vis_index, Arrays.asList(id1, link_type, true), 0,
        //        DEFAULT_LIMIT, 0);
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("getLinkList received " + id1 + "." + link_type);
        }

        if (((List)res.get(0)).isEmpty() || ((List)res.get(0)).get(0) == null)
            return null;

//        if (res.isEmpty())
//            return null;
        List<Link> links = new ArrayList<>();
        for (Object o: res){
            links.add(Converter.ListToLink((List)o));
        }

        Link[] result = new Link[links.size()];
        return links.toArray(result);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp,
                              long maxTimestamp, int offset, int limit) throws Exception {
        try {
            return getLinkListImpl(id1, link_type, minTimestamp, maxTimestamp, offset, limit);
        } catch (Exception ex) {
            logger.error("getLinkList time failed! " + ex);
            throw ex;
        }
    }

    private Link[] getLinkListImpl(long id1, long link_type, long minTimestamp, long maxTimestamp,
                                   int offset, int limit) throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("getLinkBetween " + id1 +
                    "." + link_type +
                    "." + minTimestamp +
                    "." + maxTimestamp +
                    ".off=" + offset +
                    ".lim=" + limit);
        }

        List res = syncOps.call(METHOD_GET_LINK_LIST_TIME, id1, link_type,
                minTimestamp, maxTimestamp, offset, limit);

        if (((List)res.get(0)).isEmpty()){
            return null;
        }

        List<Link> links = new ArrayList();
        for (Object o: res){
            if (o != null && ((List) o).get(0) != null)
                links.add(Converter.ListToLink((List)o));
        }

        Link[] result = new Link[links.size()];
        return links.toArray(result);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        try {
            return countLinksImpl(id1, link_type);
        } catch (Exception ex) {
            logger.error("countLinks failed! " + ex);
            throw ex;
        }
    }

    private long countLinksImpl(long id1, long link_type)
            throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("countLink " + id1 +
                    "." + link_type );
        }
        List res = syncOps.call(METHOD_COUNT_LINKS, id1, link_type);
//        List res = conn.select(schema.counts.id, schema.counts.primary, Arrays.asList(id1, link_type), 0, 1, 0);
//        if (res.isEmpty())
//            return 0;
        if (((List)res.get(0)).isEmpty() || ((List)res.get(0)).get(0) == null)
            return 0;
        return ((Number)((List)res.get(0)).get(0)).longValue();
        //return ((Number)((List)res.get(0)).get(2)).longValue();
    }

    @Override
    public int bulkLoadBatchSize() {
        return bulkInsertSize;
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        //not sure what to do here
    }

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        try {
            return addNodeImpl(dbid, node);
        } catch (Exception ex) {
            logger.error("addNode failed! " + ex);
            throw ex;
        }
    }

    private long addNodeImpl(String dbid, Node node) throws Exception {
        List l = new ArrayList<Node>();
        l.add(node);
        long ids[] = bulkAddNodes(dbid, l);
        assert(ids.length == 1);
        return ids[0];
    }

    @Override
    public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
        try {
            return bulkAddNodesImpl(nodes);
        } catch (Exception ex) {
            logger.error("bulkAddNodes failed! " + ex);
            throw ex;
        }
    }

    private long[] bulkAddNodesImpl(List<Node> nodes)
            throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("addBulkNodes: " + nodes.size() + " nodes");
        }
        List objects = new ArrayList(nodes.size());
        for (Node node: nodes){
            if (node != null)
                objects.add(Converter.NodeToList(node));
        }

        List res = syncOps.call(METHOD_ADD_BULK_NODES, objects);

        List inds = (List) res.get(0);

        long[] ans = new long[inds.size()];
        int c = 0;
        for (Object o: inds){
            ans[c] = ((Number) o).longValue();
            c++;
        }

        return ans;
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        try {
            return getNodeImpl(type, id);
        } catch (Exception ex) {
            logger.error("getnode failed! " + ex);
            throw ex;
        }
    }

    private Node getNodeImpl(int type, long id) throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("getNode: " + id + "." + type);
        }

        //List res = conn.select(schema.nodes.id, schema.nodes.primary, Arrays.asList(id), 0, 1, 0);
        List res = syncOps.call(METHOD_GET_NODE, id, type);
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("getNode received: " + id + "." + type);
        }

        if (((List)res.get(0)).isEmpty() || ((List)res.get(0)).get(0) == null){
            return null;
        }

        //        if (res.isEmpty()){
//            return null;
//        }
        Node node = Converter.ListToNode((List)res.get(0));
        return node;
//        if (node.type == type)
//            return node;
//        return null;
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("updateNode: " + node.id + "." + node.type +
            "." + node.version + "." + node.time + "." + new String(node.data));
        }
        try {
            return updateNodeImpl(node);
        } catch (Exception ex) {
            logger.error("updateNode failed! " + ex);
            throw ex;
        }
    }

    private boolean updateNodeImpl(Node node) throws Exception {
        List nodeAsList = new ArrayList();
        nodeAsList.add(node.id);
        nodeAsList.addAll(Converter.NodeToList(node));
        List res = syncOps.call(METHOD_UPDATE_NODE, nodeAsList);
        return (Boolean)((List) res.get(0)).get(0);
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("deleteNode: " + id + "." + type);
        }
        try{
            return deleteNodeImpl(id, type);
        } catch (Exception ex){
            logger.error("deleteNode failed! " + ex);
            throw ex;
        }
    }

    private boolean deleteNodeImpl(long id, int type) throws Exception {
        List res =  syncOps.call(METHOD_DELETE_NODE, id, type);
        return (Boolean)((List) res.get(0)).get(0);
    }


    @Override
    public void addBulkCounts(String dbid, List<LinkCount> counts)
            throws Exception {
        try {
            addBulkCountsImpl(counts);
        } catch (Exception ex) {
            logger.error("addbulkCounts failed! " + ex);
            throw ex;
        }
    }

    private void addBulkCountsImpl(List<LinkCount> counts)
            throws Exception {
        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
            logger.debug("addBulkCounts: " + counts.size() + " link counts");
        }
        List objects = new ArrayList(counts.size());
        for (LinkCount count: counts){
            objects.add(Converter.CountToList(count));
        }
        syncOps.call(METHOD_ADD_COUNTS, objects);
    }
}
