package edu.cmu.neuron2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Filter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoServiceConfig;
import org.apache.mina.common.IoSession;
import org.apache.mina.transport.socket.nio.DatagramAcceptor;
import org.apache.mina.transport.socket.nio.DatagramAcceptorConfig;

import edu.cmu.neuron2.Msg.RoutingRecs.Rec;
import edu.cmu.neuron2.RonTest.RunMode;

public class NeuRonNode extends Thread {
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private int myNid; // TODO non final
    private final boolean isCoordinator;
    private final String coordinatorHost;
    private final int basePort;
    private boolean doQuit;
    private final Logger logger;

    private final Hashtable<Integer, NodeInfo> nodes = new Hashtable<Integer, NodeInfo>();

    // probeTable[i] = node members[i]'s probe table. value
    // at index j in row i is the link latency between nodes members[i]->members[j].
    long[][] probeTable;
    private GridNode[][] grid;
    private int numCols, numRows;
    private final HashSet<GridNode> overflowNeighbors = new HashSet<GridNode>();
    private Hashtable<Integer, Integer> nextHopTable = new Hashtable<Integer, Integer>();
    private final IoServiceConfig cfg = new DatagramAcceptorConfig();

    private int currentStateVersion;

    public final int neighborBroadcastPeriod;
    public final int probePeriod;

    private NodeInfo coordNode = new NodeInfo();
    private DatagramSocket sendSocket;

    private RunMode mode;
    private final int numNodesHint;
    private Semaphore semAllJoined;

    public NeuRonNode(int id, ExecutorService executor, ScheduledExecutorService scheduler,
                        Properties props, int num_nodes_hint, Semaphore sem_all_joined) {
        myNid = id;
        coordNode.id = 0;
        currentStateVersion = 0;

        basePort = Integer.parseInt(props.getProperty("basePort", "9000"));
        mode = RunMode.valueOf(props.getProperty("mode", "sim").toUpperCase());
        neighborBroadcastPeriod = Integer.parseInt(props.getProperty("neighborBroadcastPeriod", "10"));
        probePeriod = Integer.parseInt(props.getProperty("probePeriod", "5"));
        timeout = Integer.parseInt(props.getProperty("timeout", "" + probePeriod * 5));
        scheme = RoutingScheme.valueOf(props.getProperty("scheme", "SIMPLE").toUpperCase());

        Formatter fmt = new Formatter() {
            public String format(LogRecord record) {
                StringBuffer buf = new StringBuffer();
                buf.append(record.getMillis()).append(' ').append(new Date(record.getMillis())).append(" ").append(
                        record.getLevel()).append(" ").append(
                        record.getLoggerName()).append(": ").append(
                        record.getMessage()).append("\n");
                return buf.toString();
            }
        };
        Logger rootLogger = Logger.getLogger("");
        rootLogger.getHandlers()[0].setFormatter(fmt);
        logger = Logger.getLogger("node" + myNid);
        if (props.getProperty("logfilter") != null) {
            String[] labels = props.getProperty("logfilter").split(" ");
            final HashSet<String> suppressedLabels = new HashSet<String>(Arrays.asList(labels));
            final boolean doConsole = !suppressedLabels.contains("all");
            rootLogger.getHandlers()[0].setFilter(new Filter() {
                public boolean isLoggable(LogRecord record) {
                    if (!doConsole) return false;
                    String[] parts = record.getLoggerName().split("\\.", 2);
                    return parts.length == 1
                            || !suppressedLabels.contains(parts[1]);
                }
            });
        }

        try {
            String logFileBase = props.getProperty("logFileBase", "%t/scaleron-log-");
            FileHandler fh = new FileHandler(logFileBase + myNid);
            fh.setFormatter(fmt);
            logger.addHandler(fh);

            sendSocket = new DatagramSocket();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        try {
            coordinatorHost = props.getProperty("coordinatorHost",
                    InetAddress.getLocalHost().getHostAddress());
            coordNode.addr = InetAddress.getByName(coordinatorHost);
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
        coordNode.port = basePort;
        this.executor = executor;
        this.scheduler = scheduler;
        probeTable = null;
        grid = null;
        numCols = numRows = 0;
        isCoordinator = myNid == 0;
//        cfg.getFilterChain().addLast("codec",
//                new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));

        numNodesHint = num_nodes_hint;
        semAllJoined = sem_all_joined;
    }

    private void log(String msg) {
        logger.info(msg);
    }

    private void warn(String msg) {
        logger.warning(msg);
    }

    private void err(String msg) {
        logger.severe(msg);
    }

    private void err(Exception ex) {
        StringWriter s = new StringWriter();
        PrintWriter p = new PrintWriter(s);
        ex.printStackTrace(p);
        err(s.toString());
    }

    /**
     * Used for logging data, such as neighbor lists.
     *
     * @param name - the name of the data, e.g.: "neighbors", "info"
     * @param value
     */
    private void log(String name, Object value) {
        Logger.getLogger(logger.getName() + "." + name).info(value.toString());
    }

    public void run() {
        if (isCoordinator) {
            try {
                int nextNodeId = 1;
                //Thread.sleep(2000);
                new DatagramAcceptor().bind(new InetSocketAddress(InetAddress
                        .getLocalHost(), basePort), new CoordReceiver(), cfg);
                ServerSocket ss = new ServerSocket(basePort);
                try {
                    // TODO the coord should also be kept aware of who's alive
                    // and who's not. this means we need to ping the coord, and
                    // the coord needs to maintain timeouts like everyone else.
                    ss.setReuseAddress(true);
                    ss.setSoTimeout(1000);
                    log("Beep!");

                    final Hashtable<Integer, Socket> incomingSocks = new Hashtable<Integer, Socket>();
                    while (!doQuit) {
                        final Socket incoming;
                        try {
                            incoming = ss.accept();
                        } catch (SocketTimeoutException ex) {
                            continue;
                        }
                        final int nodeId = nextNodeId++;

                        if (mode == RunMode.SIM) {
                            incomingSocks.put(nodeId, incoming);
                            executor.submit(new Runnable() {
                                public void run() {
                                    try {
                                        Msg.Join msg = (Msg.Join) new ObjectInputStream(incoming.getInputStream())
                                                                        .readObject();

                                        synchronized (NeuRonNode.this) {
                                            addMemberWithoutBroadcast(nodeId, msg.addr, basePort + nodeId);
                                            if (nodes.size() == numNodesHint) {
                                                // time to broadcast ims to everyone
                                                ArrayList<NodeInfo> memberList = new ArrayList<NodeInfo>(nodes.values());
                                                for (NodeInfo member : memberList) {
                                                    resetTimeoutAtCoord(member.id);
                                                    try {
                                                        Msg.Init im = new Msg.Init();
                                                        im.id = member.id;
                                                        im.version = currentStateVersion;
                                                        im.members = memberList;
                                                        new ObjectOutputStream(incomingSocks.get(member.id).getOutputStream())
                                                                .writeObject(im);
                                                    } finally {
                                                        incomingSocks.get(member.id).close();
                                                    }
                                                }
                                                semAllJoined.release();
                                            }
                                        }
                                    }  catch (Exception ex) {
                                        throw new RuntimeException(ex);
                                    }
                                }
                            });
                        }
                        else {
                            resetTimeoutAtCoord(nodeId);
                            executor.submit(new Runnable() {
                                public void run() {
                                    try {
                                        Msg.Join msg = (Msg.Join) new ObjectInputStream(incoming.getInputStream())
                                                                        .readObject();
                                        try {
                                            Msg.Init im = new Msg.Init();
                                            im.id = nodeId;
                                            synchronized (NeuRonNode.this) {
                                                addMember(nodeId, msg.addr,
                                                          basePort + nodeId);
                                                im.version = currentStateVersion;
                                                im.members = new ArrayList<NodeInfo>(nodes.values());
                                            }
                                            new ObjectOutputStream(incoming.getOutputStream())
                                                    .writeObject(im);
                                        } finally {
                                            incoming.close();
                                        }
                                    } catch (Exception ex) {
                                        throw new RuntimeException(ex);
                                    }
                                }
                            });
                        }

                    }
                } finally {
                    ss.close();
                    log("coord done");
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            try {
                Socket s;
                while (true) {
                    // Connect to the co-ordinator
                    try {
                        s = new Socket(coordinatorHost, basePort);
                        break;
                    } catch (Exception ex) {
                        log("couldn't connect to coord, retrying in 1 sec");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {

                        }
                    }
                }

                try {
                    // talk to coordinator
                    log("sending join to coordinator at " + coordinatorHost + ":" + basePort);
                    Msg.Join msg = new Msg.Join();
                    msg.addr = InetAddress.getLocalHost();
                    new ObjectOutputStream(s.getOutputStream()).writeObject(msg);

                    log("waiting for InitMsg");
                    Msg.Init im = (Msg.Init) new ObjectInputStream(s.getInputStream()).readObject();
                    assert im.id > 0;
                    myNid = im.id;
                    currentStateVersion = im.version;
                    log("got from coord => " + im);
                    updateMembers(im.members);
                } finally {
                    try {
                        s.close();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            // now start accepting pings and other msgs,
            // also start sending probes and sending out other msgs
            try {
                new DatagramAcceptor().bind(new InetSocketAddress(InetAddress.getLocalHost(), basePort + myNid),
                                            new Receiver(), cfg);
                log("server started on " + InetAddress.getLocalHost() + ":" + (basePort + myNid));
                scheduler.scheduleAtFixedRate(new Runnable() {
                    public void run() {
                        synchronized (NeuRonNode.this) {
                            try {
                                pingAll();
                            } catch (Exception ex) {
                                // failure-oblivious: swallow any exceptions and
                                // just try resuming
                                err(ex);
                            }
                        }
                    }
                }, 1, probePeriod, TimeUnit.SECONDS);
                scheduler.scheduleAtFixedRate(new Runnable() {
                    public void run() {
                        synchronized (NeuRonNode.this) {
                            try {
                                broadcastMeasurements();
                                if (scheme != RoutingScheme.SIMPLE) {
                                    if (scheme == RoutingScheme.SQRT_SPECIAL) {
                                        broadcastRecommendations2();
                                    }
                                    else {
                                        broadcastRecommendations();
                                    }
                                }
                            } catch (Exception ex) {
                                // failure-oblivious: swallow any exceptions and
                                // just try resuming
                                err(ex);
                            }
                        }
                    }
                }, 1, neighborBroadcastPeriod, TimeUnit.SECONDS);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private final HashSet<Integer> ignored = new HashSet<Integer>();

    public synchronized void ignore(int nid) {
        log("ignoring " + nid);
        ignored.add(nid);
    }

    public synchronized void unignore(int nid) {
        log("unignoring " + nid);
        ignored.remove(nid);
    }

    private void pingAll() {
        Msg.Ping ping = new Msg.Ping();
        ping.time = System.currentTimeMillis();
        ping.info = nodes.get(myNid);
        for (int nid : nodes.keySet())
            if (nid != myNid)
                sendObject(ping, nid);

        /* send ping to the membership server (co-ord) -
           this might not be required if everone makes their own local decision
           i.e. each node notices that no other node can reach a node (say X),
           then each node sends the co-ord a msg saying that "i think X is dead".
           The sending of this msg can be staggered in time so that the co-ord is not flooded with mesgs.
           The co-ordinator can then make a decision on keeping or removing node Y from the membership.
           On seeing a subsequent msg from the co-ord that X has been removed from the overlay, if a node Y
           has not sent its "i think X is dead" msg, it can cancel this event.
        */
        sendObject(ping, 0);
    }

    private Msg deserialize(Object o) {
        ByteBuffer buf = (ByteBuffer) o;
        byte[] bytes = new byte[buf.limit()];
        buf.get(bytes);
        try {
            return (Msg) new ObjectInputStream(new ByteArrayInputStream(
                    bytes)).readObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * coordinator's msg handling loop
     */
    public final class CoordReceiver extends IoHandlerAdapter {
        @Override
        public void messageReceived(IoSession session, Object obj)
                throws Exception {
            Msg msg = deserialize(obj);
            log("recv." + msg.getClass().getSimpleName(), "from " + msg.src);
            synchronized (NeuRonNode.this) {
                resetTimeoutAtCoord(msg.src);
                if (msg instanceof Msg.Ping) {
                    // ignore the ping
                } else if (msg instanceof Msg.MemberPoll) {
                    sendMembership(msg.src);
                } else {
                    throw new Exception("can't handle that message type");
                }
            }
        }
    }

    /**
     * receiver's msg handling loop
     */
    public final class Receiver extends IoHandlerAdapter {
        @Override
        public void messageReceived(IoSession session, Object obj)
                throws Exception {
            Msg msg = deserialize(obj);
            synchronized (NeuRonNode.this) {
                if (ignored.contains(msg.src)) return;
                log("recv." + msg.getClass().getSimpleName(), "from " + msg.src);
                if (msg.version > currentStateVersion) {
                    if (msg instanceof Msg.Membership) {
                        currentStateVersion = msg.version;
                        updateMembers(((Msg.Membership) msg).members);
                    } else {
                        // i am out of date - request latest membership
                        sendObject(new Msg.MemberPoll(), 0);
                    }
                } else if (msg.version == currentStateVersion) {
                    if (msg instanceof Msg.Membership) {
                        updateMembers(((Msg.Membership) msg).members);
                    } else if (msg instanceof Msg.Measurements) {
                        log(((Msg.Measurements) msg).toString());
                        updateNetworkState((Msg.Measurements) msg);
                    } else if (msg instanceof Msg.RoutingRecs) {
                        log(((Msg.RoutingRecs) msg).toString());
                        handleRecommendation(((Msg.RoutingRecs) msg).recs);
                        log(toStringNextHopTable());
                    } else if (msg instanceof Msg.Ping) {
                        Msg.Ping ping = ((Msg.Ping) msg);
                        Msg.Pong pong = new Msg.Pong();
                        pong.time = ping.time;
                        sendObject(pong, ping.src);
                    } else if (msg instanceof Msg.Pong) {
                        Msg.Pong pong = (Msg.Pong) msg;
                        resetTimeoutAtNode(pong.src);
                        int rtt = (int) (System.currentTimeMillis() - pong.time);
                        log("recv." + msg.getClass().getSimpleName(), "one way latency to " + pong.src + " = " + rtt/2);
                        ArrayList<Integer> sortedNids = memberNids();
                        probeTable[sortedNids.indexOf(myNid)][sortedNids.indexOf(pong.src)]
                                                                = rtt / 2;
                    } else if (msg instanceof Msg.PeeringRequest) {
                        Msg.PeeringRequest pr = (Msg.PeeringRequest) msg;
                        GridNode newNeighbor = new GridNode();
                        newNeighbor.id = pr.src;
                        newNeighbor.isAlive = true;
                        overflowNeighbors.add(newNeighbor);
                    } else {
                        throw new Exception("can't handle that message type");
                    }
                } else {
                    warn("stale msg from " + msg.src);
                }
            }
        }
    }

    /**
     * If we don't hear from a node for this number of seconds, then consider
     * them dead.
     */
    private int timeout;
    private Hashtable<Integer, ScheduledFuture<?>> timeouts = new Hashtable<Integer, ScheduledFuture<?>>();

    /**
     * a coord-only method
     *
     * @param nid
     */
    private void resetTimeoutAtCoord(final int nid) {
        if (nodes.contains(nid)) {
            ScheduledFuture<?> oldFuture = timeouts.get(nid);
            if (oldFuture != null) {
                oldFuture.cancel(false);
            }
            ScheduledFuture<?> future = scheduler.schedule(new Runnable() {
                public void run() {
                    synchronized (NeuRonNode.this) {
                        removeMember(nid);
                    }
                }
            }, timeout, TimeUnit.SECONDS);
            timeouts.put(nid, future);
        }
    }

    private void resetTimeoutAtNode(final int nid) {
        if (nodes.contains(nid)) {
            ScheduledFuture<?> oldFuture = timeouts.get(nid);
            if (oldFuture != null) {
                oldFuture.cancel(false);
            }
            for (int i = 0; i < numRows; i++) {
                for (int j = 0; j < numCols; j++) {
                    if (grid[i][j].id == nid) {
                        grid[i][j].isAlive = true;
                    }
                }
            }
            ScheduledFuture<?> future = scheduler.schedule(new Runnable() {
                public void run() {
                    synchronized (NeuRonNode.this) {
                        // O(n)
                        for (int i = 0; i < numRows; i++) {
                            for (int j = 0; j < numCols; j++) {
                                if (grid[i][j].id == nid) {
                                    grid[i][j].isAlive = false;
                                }
                            }
                        }
                    }
                }
            }, timeout, TimeUnit.SECONDS);
            timeouts.put(nid, future);
        }
    }

    /**
     * a coordinator-only method
     */
    private void addMember(int newNid, InetAddress addr, int port) {
        log("adding new node: " + newNid);
        NodeInfo info = new NodeInfo();
        info.id = newNid;
        info.addr = addr;
        info.port = port;
        nodes.put(newNid, info);
        currentStateVersion++;
        broadcastMembershipChange(newNid);
    }

    /**
     * a coordinator-only method - for init in simulation only
     */
    private void addMemberWithoutBroadcast(int newNid, InetAddress addr, int port) {
        log("adding new node: " + newNid);
        NodeInfo info = new NodeInfo();
        info.id = newNid;
        info.addr = addr;
        info.port = port;
        nodes.put(newNid, info);
        currentStateVersion++;
    }

    private ArrayList<Integer> memberNids() {
        ArrayList<Integer> nids = new ArrayList<Integer>(nodes.keySet());
        Collections.sort(nids);
        return nids;
    }

    /**
     * a coordinator-only method
     *
     * @param exceptNid
     */
    private void broadcastMembershipChange(int exceptNid) {
        for (int nid : nodes.keySet()) {
            if (nid != exceptNid) {
                sendMembership(nid);
            }
        }
    }

    /**
     * a coordinator-only method
     */
    private void sendMembership(int nid) {
        Msg.Membership msg = new Msg.Membership();
        msg.members = new ArrayList<NodeInfo>(nodes.values());
        sendObject(msg, nid);
    }

    /**
     * a coordinator-only method
     *
     * @param nid
     */
    private void removeMember(int nid) {
        log("removing dead node " + nid);
        nodes.remove(nid);
        currentStateVersion++;
        broadcastMembershipChange(nid);
    }

    private void updateMembers(List<NodeInfo> newNodes) {
        List<Integer> oldNids = memberNids();
        nodes.clear();

        for (NodeInfo node : newNodes) {
            nodes.put(node.id, node);
        }

        Hashtable<Integer, Integer> newNextHopTable = new Hashtable<Integer, Integer>(nodes.size());
        for (NodeInfo node : newNodes) {
            if (node.id != myNid) {
                Integer nextHop = nextHopTable.get(node.id);
                if (nextHop == null) {
                    // new node !
                    newNextHopTable.put(node.id, myNid);
                }
                else {
                    // check if this old next hop is in the new membership list
                    if (nodes.get(nextHop) != null) {
                        // we have some next hop that is alive - leave it as is
                        newNextHopTable.put(node.id, nextHop);
                    }
                    else {
                        // the next hop vanaished. i am next hop to this node now
                        newNextHopTable.put(node.id, myNid);
                    }
                }
            }
            else {
                newNextHopTable.put(myNid, myNid);
            }
        }
        nextHopTable = newNextHopTable; // forget about the old one

        repopulateGrid();
        repopulateProbeTable(oldNids);
        printGrid();
        log(toStringNeighborList());
    }

    /**
     * TODO XXX OPEN QUESTION HOW TO HANDLE NODE WORLD VIEW INCONSISTENCIES????
     */
    private void repopulateGrid() {
        numCols = (int) Math.ceil(Math.sqrt(nodes.size()));
        numRows = (int) Math.ceil((double) nodes.size() / (double) numCols);
        grid = new GridNode[numRows][numCols];
        List<Integer> nids = memberNids();
        int m = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                if (m >= nids.size()) {
                    m = 0;
                }
                GridNode gn = new GridNode();
                gn.id = nids.get(m);
                gn.isAlive = true;
                grid[i][j] = gn;
                m++;
            }
        }
        overflowNeighbors.clear();
        // repopulateNeighborList();
    }

    public static enum RoutingScheme { SIMPLE, SQRT, SQRT_SPECIAL };
    private final RoutingScheme scheme;

    private HashSet<GridNode> getNeighborList() {
        HashSet<GridNode> neighborSet = new HashSet<GridNode>();
        for (int r = 0; r < numRows; r++) {
            for (int c = 0; c < numCols; c++) {

                // this can happen at most twice
                if (scheme == RoutingScheme.SIMPLE) {
                    neighborSet.add(grid[r][c]);
                } else if (grid[r][c].id == myNid) {
                    // all the nodes in row i, and all the nodes in column j are
                    // belong to us :)

                    // O(N^1.5)   :(
                    for (int x = 0; x < numCols; x++) {
                        if (grid[r][x].id != myNid) {
                            GridNode neighbor = grid[r][x];
                            if (neighbor.isAlive) {
                                neighborSet.add(neighbor);
                            } else {
                                log("R node failover!");
                                for (int i = 0; i < numRows; i++) {
                                    if ( (i != r) && (grid[i][c].isAlive == false) ) {
                                        /* (r, x) and (i, c) can't be reached
                                         * (i, x) needs a failover R node
                                         */
                                        for (int j = 0; j < numCols; j++) {
                                            if ( (grid[i][j].id != myNid) && (grid[i][j].isAlive == true) ) {
                                                Msg.PeeringRequest pr = new Msg.PeeringRequest();
                                                sendObject(pr, grid[i][j].id);
                                                neighborSet.add(grid[i][j]);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for (int x = 0; x < numRows; x++) {
                        if (grid[x][c].id != myNid) {
                            neighborSet.add(grid[x][c]);
                        }
                    }
                }

            }
        }
        return neighborSet;
    }

    private HashSet<GridNode> getOtherMembers() {
        HashSet<GridNode> memberSet = new HashSet<GridNode>();
        for (int r = 0; r < numRows; r++) {
            for (int c = 0; c < numCols; c++) {
                if (grid[r][c].id != myNid) {
                    memberSet.add(grid[r][c]);
                }
            }
        }
        return memberSet;
    }

    /**
     * expands the probes table to reflect changes in the new membership view.
     * assumes that "nodes" has been updated with the new membership. copies
     * over probe info from previous table for the nodes that are common across
     * the two membership views.
     */
    private void repopulateProbeTable(List<Integer> oldNids) {
        long newProbeTable[][] = new long[nodes.size()][nodes.size()];

        int nodeIndex = memberNids().indexOf(myNid);
        for (int i = 0; i < memberNids().size(); i++) {
            if (i == nodeIndex) {
                newProbeTable[i][i] = 0;
            } else {
                newProbeTable[nodeIndex][i] = Integer.MAX_VALUE;
            }
        }

        // copy over old probe data.
        for (int i = 0; i < oldNids.size(); i++) {
            int node_index = memberNids().indexOf(oldNids.get(i));
            if (node_index != -1) {
                for (int j = 0; j < oldNids.size(); j++) {
                    int node_index_2 = memberNids().indexOf(oldNids.get(j));
                    if (node_index_2 != -1)
                        newProbeTable[node_index][node_index_2] = probeTable[i][j];
                }
            }
        }

        probeTable = newProbeTable; // forget about the old one.

        /*
        // for testing
        if (nodeIndex == 0) {
            for (int i = 1; i < memberNids().size(); i++) {
                probeTable[nodeIndex][i] = 1;
            }
        } else {
            probeTable[nodeIndex][0] = 1;
        }
        */
    }

    private String toStingMembership() {
        String s = new String("Membership for Node " + myNid
                + ". Membership = [");
        for (Integer memberId : memberNids()) {
            s += memberId + ", ";
        }
        s += "]";
        return s;
    }

    private String toStringNeighborList() {
        String s = new String("Neighbors for Node " + myNid
                + ". Neighbors = [");
        HashSet<GridNode> neighbors = getNeighborList();
        for (GridNode neighbor : neighbors) {
            s += neighbor.id + ", ";
        }
        s += "]";
        return s;
    }

    private String toStringNextHopTable() {
        String s = new String("Next-hop table for " + myNid
                + " = [");
        for (Integer node : nextHopTable.keySet()) {
            s += node + " -> " + nextHopTable.get(node) + "; ";
        }
        s += "]";
        return s;
    }

    private void printGrid() {
        String s = new String("Grid for Node " + myNid + ".\n");
        if (grid != null) {
            for (int i = 0; i < numRows; i++) {
                for (int j = 0; j < numCols; j++) {
                    s += "\t " + grid[i][j];
                }
                s += "\n";
            }
        }
        log(s);
    }

    /**
     * for each neighbor, find for him the min-cost hops to all other neighbors,
     * and send this info to him (the intermediate node may be one of the
     * endpoints, meaning a direct route is cheapest)
     */
    private void broadcastRecommendations() {
        HashSet<GridNode> nl = getNeighborList();
        nl.addAll(overflowNeighbors);
        overflowNeighbors.clear();
        log("Sending recommendations to neighbors. " + toStringNeighborList());
        ArrayList<Integer> sortedNids = memberNids();
        for (GridNode src : nl) {
            int srcOffset = sortedNids.indexOf(src.id);
            ArrayList<Msg.RoutingRecs.Rec> recs = new ArrayList<Msg.RoutingRecs.Rec>();
            long min = Long.MAX_VALUE;
            int mini = -1;
            for (GridNode dst : nl) {
                int dstOffset = sortedNids.indexOf(dst.id);
                if (src.id != dst.id) {
                    for (int i = 0; i < probeTable[srcOffset].length; i++) {
                        // we assume bi-directional links for the time being
                        // i.e. link from a-> b is the same as b -> a
                        long cur = probeTable[srcOffset][i] + probeTable[dstOffset][i];
                        if (cur < min) {
                            min = cur;
                            mini = i;
                        }
                    }
                    recs.add(new Msg.RoutingRecs.Rec(dst.id, mini));
                }
            }
            Msg.RoutingRecs msg = new Msg.RoutingRecs();
            msg.recs = recs;
            sendObject(msg, src.id);
        }
    }

    /**
     * for each neighbor, find for him the min-cost hops to *all other nodes* (as opposed to neighbors),
     * and send this info to him (the intermediate node may be one of the
     * endpoints, meaning a direct route is cheapest)
     */
    private void broadcastRecommendations2() {
        HashSet<GridNode> nl = getNeighborList();
        nl.addAll(overflowNeighbors);
        overflowNeighbors.clear();
        log("Sending recommendations to neighbors. " + toStringNeighborList());
        ArrayList<Integer> sortedNids = memberNids();

        HashSet<GridNode> others = getOtherMembers();
        others.removeAll(nl);

        for (GridNode src : nl) {
            int srcOffset = sortedNids.indexOf(src.id);
            ArrayList<Msg.RoutingRecs.Rec> recs = new ArrayList<Msg.RoutingRecs.Rec>();

            // src = neighbor, dst = neighbor
            for (GridNode dst : nl) {
                int dstOffset = sortedNids.indexOf(dst.id);
                long min = Long.MAX_VALUE;
                int mini = -1;
                if (src.id != dst.id) {
                    for (int i = 0; i < probeTable[srcOffset].length; i++) {
                        // we assume bi-directional links for the time being
                        // i.e. link from a-> b is the same as b -> a
                        long cur = probeTable[srcOffset][i] + probeTable[dstOffset][i];
                        if (cur < min) {
                            min = cur;
                            mini = sortedNids.get(i);
                        }
                    }
                    recs.add(new Msg.RoutingRecs.Rec(dst.id, mini));
                }
            }

            // src = neighbor, dst != neighbor
            for (GridNode dst : others) {
                int dstOffset = sortedNids.indexOf(dst.id);
                long min = probeTable[srcOffset][dstOffset];
                int mini = srcOffset;
                if (src.id != dst.id) {
                    for (GridNode neighborHop : nl) {
                        int neighborHopOffset = sortedNids.indexOf(neighborHop.id);
                        long curMin = probeTable[srcOffset][neighborHopOffset] + probeTable[neighborHopOffset][dstOffset];
                        if (curMin < min) {
                            min = curMin;
                            mini = neighborHop.id;
                        }
                    }
                    recs.add(new Msg.RoutingRecs.Rec(dst.id, mini));
                }
            }

            Msg.RoutingRecs msg = new Msg.RoutingRecs();
            msg.recs = recs;
            sendObject(msg, src.id);
        }
    }


    private void sendObject(final Msg o, int nid) {
        if (nid != myNid) {
            NodeInfo node = nid == 0 ? coordNode : nodes.get(nid);
            o.src = myNid;
            o.version = currentStateVersion;

            try {
                /*
                 * note that it's unsafe to re-use these output streams - at
                 * least, i don't know how (reset() is insufficient)
                 */
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(o);
                byte[] buf = baos.toByteArray();
                log("send." + o.getClass().getSimpleName(),
                        String.format("to %d at %s:%d, version %d, len %d",
                            nid,
                            node.addr,
                            node.port,
                            currentStateVersion,
                            buf.length));
                sendSocket.send(new DatagramPacket(buf, buf.length, node.addr, node.port));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void broadcastMeasurements() {
        Msg.Measurements rm = new Msg.Measurements();
        rm.membershipList = memberNids();
        rm.probeTable = probeTable[rm.membershipList.indexOf(myNid)].clone();
        HashSet<GridNode> nl = getNeighborList();
        log("Sending measurements to neighbors. " + toStringNeighborList());
        for (GridNode neighbor : nl) {
            sendObject(rm, neighbor.id);
        }
    }

    private void updateNetworkState(Msg.Measurements m) {
        int offset = memberNids().indexOf(m.src);
        // Make sure that we have the exact same world-views before proceeding,
        // as otherwise the neighbor sets may be completely different. Steps can
        // be taken to tolerate differences and to give best-recommendations
        // based on incomplete info, but it may be better to take a step back
        // and re-evaluate our approach to consistency as a whole first. For
        // now, this simple central-coordinator approach will at least work.
        if (offset != -1 && m.membershipList.equals(memberNids())) {
            for (int i = 0; i < m.probeTable.length; i++) {
                probeTable[offset][i] = m.probeTable[i];
            }
        }
    }

    private void handleRecommendation(ArrayList<Rec> recs) {
        if (recs != null) {
            for (Rec r : recs) {
                // For the algorithm where the R-points only send recos about their neighbors:
                // For each dst - only 2 nodes can tell us about the best hop to dst.
                // They are out R-points. Trust them and update your entry blindly.
                // For the algorithm where the R-points only send recos about
                //    everyone else this logic will have to be more complex
                //	  (like check if the reco was better)
                nextHopTable.put(r.dst, r.via);
            }
        }
    }

    public void quit() {
        this.doQuit = true;
    }

}

class GridNode {
    public int id;
    public boolean isAlive;

    public String toString() {
        return id + (isAlive ? "(up)" : "(DOWN)");
    }

    public int hashCode() {
        return new Integer(id).hashCode();
    }

    public boolean equals(Object other) {
        if (other != null && getClass() == other.getClass()) {
            GridNode otherItem = (GridNode) other;
            return (otherItem.id == this.id)
                    && (otherItem.isAlive == this.isAlive);
        } else
            return false;
    }
}
