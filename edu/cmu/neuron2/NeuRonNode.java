package edu.cmu.neuron2;

import java.io.*;
import java.net.*;
import java.util.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

import edu.cmu.neuron2.RonTest.RunMode;

public class NeuRonNode extends Thread {
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private int myNid; // TODO non final
    private final boolean isCoordinator;
    private final String coordinatorHost;
    private final int basePort;
    private final AtomicBoolean doQuit = new AtomicBoolean();
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
        // the node id has to be logged here because
        // the node id received in the constructor (and passed to Logger) is different
        // from that in the InitMsg
        // This is the correct node ID !!!
        logger.info("{NODE " + myNid + "} " + msg);
    }

    private void warn(String msg) {
        logger.warning("{NODE " + myNid + "} " + msg);
    }

    private void err(String msg) {
        logger.severe("{NODE " + myNid + "} " + msg);
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
                    while (!doQuit.get()) {
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
                                        Join msg = (Join) Serialization.deserialize(new DataInputStream(incoming.getInputStream()));

                                        synchronized (NeuRonNode.this) {
                                            addMemberWithoutBroadcast(nodeId, msg.addr, basePort + nodeId);
                                            if (nodes.size() == numNodesHint) {
                                                // time to broadcast ims to everyone
                                                ArrayList<NodeInfo> memberList = new ArrayList<NodeInfo>(nodes.values());
                                                for (NodeInfo member : memberList) {
                                                    resetTimeoutAtCoord(member.id);
                                                    try {
                                                        Init im = new Init();
                                                        im.id = member.id;
                                                        im.version = currentStateVersion;
                                                        im.members = memberList;
                                                        DataOutputStream dos = new DataOutputStream(incomingSocks.get(member.id).getOutputStream());
                                                        Serialization.serialize(im, dos);
                                                        dos.flush();
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
                                        Join msg = (Join) new ObjectInputStream(incoming.getInputStream())
                                                                        .readObject();
                                        try {
                                            Init im = new Init();
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
                    Join msg = new Join();
                    msg.addr = InetAddress.getLocalHost();
                    DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                    Serialization.serialize(msg, dos);
                    dos.flush();

                    log("waiting for InitMsg");
                    Init im = (Init) Serialization.deserialize(new DataInputStream(s.getInputStream()));
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
        Ping ping = new Ping();
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
            return (Msg) Serialization.deserialize(new DataInputStream(new
                        ByteArrayInputStream(bytes)));
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
                if (msg instanceof Ping) {
                    // ignore the ping
                } else if (msg instanceof MemberPoll) {
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
                    if (msg instanceof Membership) {
                        currentStateVersion = msg.version;
                        updateMembers(((Membership) msg).members);
                    } else {
                        // i am out of date - request latest membership
                        sendObject(new MemberPoll(), 0);
                    }
                } else if (msg.version == currentStateVersion) {
                    if (msg instanceof Membership) {
                        updateMembers(((Membership) msg).members);
                    } else if (msg instanceof Measurements) {
                        log(((Measurements) msg).toString());
                        updateNetworkState((Measurements) msg);
                    } else if (msg instanceof RoutingRecs) {
                        log(((RoutingRecs) msg).toString());
                        handleRecommendation(((RoutingRecs) msg).recs);
                        log(toStringNextHopTable());
                    } else if (msg instanceof Ping) {
                        Ping ping = ((Ping) msg);
                        Pong pong = new Pong();
                        pong.time = ping.time;
                        sendObject(pong, ping.src);
                    } else if (msg instanceof Pong) {
                        Pong pong = (Pong) msg;
                        resetTimeoutAtNode(pong.src);
                        int rtt = (int) (System.currentTimeMillis() - pong.time);
                        log("recv." + msg.getClass().getSimpleName(), "one way latency to " + pong.src + " = " + rtt/2);
                        ArrayList<Integer> sortedNids = memberNids();
                        probeTable[sortedNids.indexOf(myNid)][sortedNids.indexOf(pong.src)]
                                                                = rtt / 2;
                    } else if (msg instanceof PeeringRequest) {
                        PeeringRequest pr = (PeeringRequest) msg;
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
        Membership msg = new Membership();
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
                                                PeeringRequest pr = new PeeringRequest();
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

    // PERF
    private void printGrid() {
//        String s = new String("Grid for Node " + myNid + ".\n");
//        if (grid != null) {
//            for (int i = 0; i < numRows; i++) {
//                for (int j = 0; j < numCols; j++) {
//                    s += "\t " + grid[i][j];
//                }
//                s += "\n";
//            }
//        }
//        log(s);
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
            ArrayList<Rec> recs = new ArrayList<Rec>();
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
                    Rec rec = new Rec();
                    rec.dst = dst.id;
                    rec.via = mini;
                    recs.add(rec);
                }
            }
            RoutingRecs msg = new RoutingRecs();
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
            ArrayList<Rec> recs = new ArrayList<Rec>();

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
                    Rec rec = new Rec();
                    rec.dst = dst.id;
                    rec.via = mini;
                    recs.add(rec);
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
                    Rec rec = new Rec();
                    rec.dst = dst.id;
                    rec.via = mini;
                    recs.add(rec);
                }
            }

            RoutingRecs msg = new RoutingRecs();
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
                Serialization.serialize(o, new DataOutputStream(baos));
                byte[] buf = baos.toByteArray();
                log("send." + o.getClass().getSimpleName() + " to " + nid + " len " + buf.length);
                sendSocket.send(new DatagramPacket(buf, buf.length, node.addr, node.port));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            /*
              leave this commented region here
              This is was not a good idea because it would open a new socket every time.
              we now instead use a single socket (sendSocket).
            */
//            new DatagramConnector().connect(new InetSocketAddress(node.addr, node.port),
//                                            new IoHandlerAdapter() {
//                @Override
//                public void sessionCreated(IoSession session) {
//                    session.write(o); // TODO :: need custom serialization
//                }
//            }, cfg);
        }
    }

    private void broadcastMeasurements() {
        Measurements rm = new Measurements();
        rm.membershipList = memberNids();
        rm.probeTable = probeTable[rm.membershipList.indexOf(myNid)].clone();
        HashSet<GridNode> nl = getNeighborList();
        log("Sending measurements to neighbors. " + toStringNeighborList());
        for (GridNode neighbor : nl) {
            sendObject(rm, neighbor.id);
        }
    }

    private void updateNetworkState(Measurements m) {
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
        this.doQuit.set(true);
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















///////////////////////////////////////
//
//
//
//
//
// welcome to my
//        DEATH MACHINE,
//           interloper!!!!!!!11
//
//
//
//
//
//
/////////////////////////////////////












class NodeInfo  {
int id;
int port;
InetAddress addr;
}
class Rec  {
int dst;
int via;
}
class Msg  {
int src;
int version;
}
class Join extends Msg {
InetAddress addr;
}
class Init extends Msg {
int id;
ArrayList<NodeInfo> members;
}
class Membership extends Msg {
ArrayList<NodeInfo> members;
int numNodes;
}
class RoutingRecs extends Msg {
ArrayList<Rec> recs;
}
class Ping extends Msg {
long time;
NodeInfo info;
}
class Pong extends Msg {
long time;
}
class Measurements extends Msg {
ArrayList<Integer> membershipList;
long[] probeTable;
}
class MemberPoll extends Msg {
}
class PeeringRequest extends Msg {
}

      class Serialization {
    

      public static void serialize(Object obj, DataOutputStream out) throws IOException {
      if (false) {}
      
else if (obj.getClass() == NodeInfo.class) {
NodeInfo casted = (NodeInfo) obj; out.writeInt(0);
out.writeInt(casted.id);
out.writeInt(casted.port);
byte[] buf = casted.addr.getAddress();out.writeInt(buf.length);out.write(buf);
}
else if (obj.getClass() == Rec.class) {
Rec casted = (Rec) obj; out.writeInt(1);
out.writeInt(casted.dst);
out.writeInt(casted.via);
}
else if (obj.getClass() == Msg.class) {
Msg casted = (Msg) obj; out.writeInt(2);
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == Join.class) {
Join casted = (Join) obj; out.writeInt(3);
byte[] buf = casted.addr.getAddress();out.writeInt(buf.length);out.write(buf);
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == Init.class) {
Init casted = (Init) obj; out.writeInt(4);
out.writeInt(casted.id);
 out.writeInt(casted.members.size()); 
for (int i = 0; i < casted.members.size(); i++) {
out.writeInt(casted.members.get(i).id);
out.writeInt(casted.members.get(i).port);
byte[] buf = casted.members.get(i).addr.getAddress();out.writeInt(buf.length);out.write(buf);
}
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == Membership.class) {
Membership casted = (Membership) obj; out.writeInt(5);
 out.writeInt(casted.members.size()); 
for (int i = 0; i < casted.members.size(); i++) {
out.writeInt(casted.members.get(i).id);
out.writeInt(casted.members.get(i).port);
byte[] buf = casted.members.get(i).addr.getAddress();out.writeInt(buf.length);out.write(buf);
}
out.writeInt(casted.numNodes);
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == RoutingRecs.class) {
RoutingRecs casted = (RoutingRecs) obj; out.writeInt(6);
 out.writeInt(casted.recs.size()); 
for (int i = 0; i < casted.recs.size(); i++) {
out.writeInt(casted.recs.get(i).dst);
out.writeInt(casted.recs.get(i).via);
}
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == Ping.class) {
Ping casted = (Ping) obj; out.writeInt(7);
out.writeLong(casted.time);
out.writeInt(casted.info.id);
out.writeInt(casted.info.port);
byte[] buf = casted.info.addr.getAddress();out.writeInt(buf.length);out.write(buf);
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == Pong.class) {
Pong casted = (Pong) obj; out.writeInt(8);
out.writeLong(casted.time);
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == Measurements.class) {
Measurements casted = (Measurements) obj; out.writeInt(9);
 out.writeInt(casted.membershipList.size()); 
for (int i = 0; i < casted.membershipList.size(); i++) {
out.writeInt(casted.membershipList.get(i));
}
out.writeInt(casted.probeTable.length);
for (int i = 0; i < casted.probeTable.length; i++) {
out.writeLong(casted.probeTable[i]);
}
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == MemberPoll.class) {
MemberPoll casted = (MemberPoll) obj; out.writeInt(10);
out.writeInt(casted.src);
out.writeInt(casted.version);
}
else if (obj.getClass() == PeeringRequest.class) {
PeeringRequest casted = (PeeringRequest) obj; out.writeInt(11);
out.writeInt(casted.src);
out.writeInt(casted.version);
}
}

      public static Object deserialize(DataInputStream in) throws IOException {
      switch (in.readInt()) {
    
case 0: { // NodeInfo
NodeInfo obj;
{
obj = new NodeInfo();
{
obj.id = in.readInt();
}
{
obj.port = in.readInt();
}
{
byte[] buf;
{

          buf = new byte[in.readInt()];
          in.read(buf);
        
}

        obj.addr = InetAddress.getByAddress(buf);
        
}
}
return obj;}
case 1: { // Rec
Rec obj;
{
obj = new Rec();
{
obj.dst = in.readInt();
}
{
obj.via = in.readInt();
}
}
return obj;}
case 2: { // Msg
Msg obj;
{
obj = new Msg();
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
return obj;}
case 3: { // Join
Join obj;
{
obj = new Join();
{
byte[] buf;
{

          buf = new byte[in.readInt()];
          in.read(buf);
        
}

        obj.addr = InetAddress.getByAddress(buf);
        
}
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}
case 4: { // Init
Init obj;
{
obj = new Init();
{
obj.id = in.readInt();
}
{
obj.members = new ArrayList<NodeInfo>();
for (int i = 0, len = in.readInt(); i < len; i++) {
NodeInfo x;
{
x = new NodeInfo();
{
x.id = in.readInt();
}
{
x.port = in.readInt();
}
{
byte[] buf;
{

          buf = new byte[in.readInt()];
          in.read(buf);
        
}

        x.addr = InetAddress.getByAddress(buf);
        
}
}
obj.members.add(x);
}
}
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}
case 5: { // Membership
Membership obj;
{
obj = new Membership();
{
obj.members = new ArrayList<NodeInfo>();
for (int i = 0, len = in.readInt(); i < len; i++) {
NodeInfo x;
{
x = new NodeInfo();
{
x.id = in.readInt();
}
{
x.port = in.readInt();
}
{
byte[] buf;
{

          buf = new byte[in.readInt()];
          in.read(buf);
        
}

        x.addr = InetAddress.getByAddress(buf);
        
}
}
obj.members.add(x);
}
}
{
obj.numNodes = in.readInt();
}
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}
case 6: { // RoutingRecs
RoutingRecs obj;
{
obj = new RoutingRecs();
{
obj.recs = new ArrayList<Rec>();
for (int i = 0, len = in.readInt(); i < len; i++) {
Rec x;
{
x = new Rec();
{
x.dst = in.readInt();
}
{
x.via = in.readInt();
}
}
obj.recs.add(x);
}
}
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}
case 7: { // Ping
Ping obj;
{
obj = new Ping();
{
obj.time = in.readLong();
}
{
obj.info = new NodeInfo();
{
obj.info.id = in.readInt();
}
{
obj.info.port = in.readInt();
}
{
byte[] buf;
{

          buf = new byte[in.readInt()];
          in.read(buf);
        
}

        obj.info.addr = InetAddress.getByAddress(buf);
        
}
}
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}
case 8: { // Pong
Pong obj;
{
obj = new Pong();
{
obj.time = in.readLong();
}
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}
case 9: { // Measurements
Measurements obj;
{
obj = new Measurements();
{
obj.membershipList = new ArrayList<Integer>();
for (int i = 0, len = in.readInt(); i < len; i++) {
Integer x;
{
x = in.readInt();
}
obj.membershipList.add(x);
}
}
{
obj.probeTable = new long[in.readInt()];
for (int i = 0; i < obj.probeTable.length; i++) {
{
obj.probeTable[i] = in.readLong();
}
}
}
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}
case 10: { // MemberPoll
MemberPoll obj;
{
obj = new MemberPoll();
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}
case 11: { // PeeringRequest
PeeringRequest obj;
{
obj = new PeeringRequest();
{
{
obj.src = in.readInt();
}
{
obj.version = in.readInt();
}
}
}
return obj;}

    default:throw new RuntimeException("unknown obj type");}}

    public static void main(String[] args) throws IOException {
{
     ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      Pong pong = new Pong();
      pong.src = 2;
      pong.version = 3;
      pong.time = 4;
      serialize(pong, out);
      byte[] buf = baos.toByteArray();
      System.out.println(buf.length);
      Object obj = deserialize(new DataInputStream(new ByteArrayInputStream(buf)));
      System.out.println(obj);
}

{
     ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);

      Measurements m = new Measurements();
      m.src = 2;
      m.version = 3;
      m.membershipList = new ArrayList<Integer>();
      m.membershipList.add(4);
      m.membershipList.add(5);
      m.membershipList.add(6);
      m.probeTable = new long[5];
      m.probeTable[1] = 7;
      m.probeTable[2] = 8;
      m.probeTable[3] = 9;

      serialize(m, out);
      byte[] buf = baos.toByteArray();
      System.out.println(buf.length);
      Object obj = deserialize(new DataInputStream(new ByteArrayInputStream(buf)));
      System.out.println(obj);
}
{
  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  DataOutputStream out = new DataOutputStream(baos);

  Membership m = new Membership();
  m.src = 2;
  m.version = 3;
  m.members = new ArrayList<NodeInfo>();
  NodeInfo n1 = new NodeInfo();
  n1.addr = InetAddress.getLocalHost();
  n1.port = 4;
  n1.id = 5;
  m.members.add(n1);
  NodeInfo n2 = new NodeInfo();
  n2.addr = InetAddress.getByName("google.com");
  n2.port = 6;
  n2.id = 7;
  m.members.add(n2);
  m.numNodes = 8;

  serialize(m, out);
  byte[] buf = baos.toByteArray();
  System.out.println(buf.length);
  Object obj = deserialize(new DataInputStream(
    new ByteArrayInputStream(buf)));
  System.out.println(obj);
}
    }
    }

