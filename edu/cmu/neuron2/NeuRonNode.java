package edu.cmu.neuron2;

import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.annotation.*;

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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.util.logging.Formatter;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoServiceConfig;
import org.apache.mina.common.IoSession;
import org.apache.mina.transport.socket.nio.DatagramAcceptor;
import org.apache.mina.transport.socket.nio.DatagramAcceptorConfig;

import edu.cmu.neuron2.RonTest.RunMode;

class LabelFilter implements Filter {
    private final HashSet<String> suppressedLabels;
    private final boolean suppressAll;
    public LabelFilter(HashSet<String> suppressedLabels) {
        this.suppressedLabels = suppressedLabels;
        this.suppressAll = suppressedLabels.contains("all");
    }
    public boolean isLoggable(LogRecord record) {
        if (suppressAll) return false;
        String[] parts = record.getLoggerName().split("\\.", 2);
        return parts.length == 1
                || !suppressedLabels.contains(parts[1]);
    }
}

public class NeuRonNode extends Thread {
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    public short myNid;
    private final boolean isCoordinator;
    private final String coordinatorHost;
    private final int basePort;
    private final AtomicBoolean doQuit = new AtomicBoolean();
    private Logger logger;

    /**
     * maps node id's to nodestates. this is the primary container.
     */
    private final Hashtable<Short, NodeState> nodes = new Hashtable<Short, NodeState>();

    /**
     * neighbors = rendesvousServers union rendezvousClients. we send our
     * routes to all servers in this set.
     */

    /**
     * maps nid to {the set of rendezvous servers to that nid}
     */
    private final Hashtable<Short, HashSet<NodeState>> rendezvousServers = new Hashtable<Short, HashSet<NodeState>>();

    /**
     * the set of nodes that are relying us to get to someone.
     *
     * this is needed during route computation. i need to know who to calculate
     * routes among, and we want to include rendezvousclients in this set.
     */
    private final SortedSet<NodeState> rendezvousClients = new TreeSet<NodeState>();

    private NodeState[][] grid;
    private short numCols, numRows;

    private final Hashtable<InetAddress, Short> addr2id = new Hashtable<InetAddress, Short>();

    private final Hashtable<Short, HashSet<NodeState>> defaultRendezvousServers =
        new Hashtable<Short, HashSet<NodeState>>();

    private short currentStateVersion;

    public final int neighborBroadcastPeriod;
    public final int probePeriod;

    private final NodeInfo coordNode;
    private final DatagramSocket sendSocket;

    private final RunMode mode;
    private final short numNodesHint;
    private final Semaphore semAllJoined;

    private final Random rand = new Random();

    private final InetAddress myCachedAddr;
    private ArrayList<Short> cachedMemberNids = new ArrayList<Short>(); // sorted list of members
    private short cachedMemberNidsVersion;
    private final boolean blockJoins;
    private final boolean capJoins;
    private final int joinTimeLimit; // seconds

    private final int dumpPeriod;

    private final FileHandler fh;
    private final short origNid;

    private final short sessionId;
    private final int linkTimeout;

    private final int membershipBroadcastPeriod;

    private static final String defaultLabelSet = "send.Ping recv.Ping stale.Ping send.Pong recv.Pong stale.Pong send.Measurements send.RoutingRecs";

    private final Hashtable<Short,Long> lastSentMbr = new Hashtable<Short,Long>();

    private final double smoothingFactor;
    private final short resetLatency = Short.MAX_VALUE;

    private final Hashtable<Short, NodeInfo> coordNodes = new Hashtable<Short, NodeInfo>();

    private final ArrayList<Short> memberNids = new ArrayList<Short>();

    private final ArrayList<NodeState> otherNodes = new ArrayList<NodeState>();

    private final ArrayList<NodeState> lastNeighbors = new ArrayList<NodeState>();

    private Runnable safeRun(final Runnable r) {
        return new Runnable() {
            public void run() {
                try {
                    synchronized (NeuRonNode.this) {
                        r.run();
                    }
                } catch (Exception ex) {
                    err(ex);
                }
            }
        };
    }

    private void createLabelFilter(Properties props, String labelSet, Handler handler) {
        String[] labels = props.getProperty(labelSet, defaultLabelSet).split(" ");
        final HashSet<String> suppressedLabels = new HashSet<String>(Arrays.asList(labels));
        handler.setFilter(new LabelFilter(suppressedLabels));
    }

    public NeuRonNode(short id, ExecutorService executor, ScheduledExecutorService scheduler,
                        Properties props, short numNodes, Semaphore semJoined,
                        InetAddress myAddr, String coordinatorHost, NodeInfo coordNode) {

        if ((coordNode == null) || (coordNode.addr == null)){
            throw new RuntimeException("coordNode is null!");
        }

        dumpPeriod = Integer.parseInt(props.getProperty("dumpPeriod", "60"));

        myNid = id;
        origNid = id;
        currentStateVersion = (short)0;
        cachedMemberNidsVersion = (short)-1;
        joinTimeLimit = Integer.parseInt(props.getProperty("joinTimeLimit", "10")); // wait up to 10 secs by default for coord to be available
        membershipBroadcastPeriod = Integer.parseInt(props.getProperty("membershipBroadcastPeriod", "0"));

        // NOTE note that you'll probably want to set this, always!
        sessionId = Short.parseShort(props.getProperty("sessionId", "0"));

        blockJoins = Boolean.valueOf(props.getProperty("blockJoins", "true"));
        capJoins = Boolean.valueOf(props.getProperty("capJoins", "true"));

        this.coordinatorHost = coordinatorHost;
        this.coordNode = coordNode;

        basePort = Integer.parseInt(props.getProperty("basePort", "9000"));
        mode = RunMode.valueOf(props.getProperty("mode", "sim").toUpperCase());
        neighborBroadcastPeriod = Integer.parseInt(props.getProperty("neighborBroadcastPeriod", "60"));

        // for simulations we can safely reduce the probing frequency, or even turn it off
        if (mode == RunMode.SIM) {
            probePeriod = Integer.parseInt(props.getProperty("probePeriod", "60"));
        } else {
            probePeriod = Integer.parseInt(props.getProperty("probePeriod", "10"));
        }
        membershipTimeout = Integer.parseInt(props.getProperty("timeout", "" + probePeriod * 3));
        linkTimeout = Integer.parseInt(props.getProperty("failoverTimeout", "" + membershipTimeout));
        scheme = RoutingScheme.valueOf(props.getProperty("scheme", "SIMPLE").toUpperCase());

        smoothingFactor = Double.parseDouble(props.getProperty("smoothingFactor", "0.9"));

        Formatter fmt = new Formatter() {
            public String format(LogRecord record) {
                StringBuilder buf = new StringBuilder();
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
        createLabelFilter(props, "consoleLogFilter", rootLogger.getHandlers()[0]);

        try {
            String logFileBase = props.getProperty("logFileBase", "%t/scaleron-log-");
            fh = new FileHandler(logFileBase + myNid, true);
            fh.setFormatter(fmt);
            createLabelFilter(props, "fileLogFilter", fh);
            logger.addHandler(fh);

            sendSocket = new DatagramSocket();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        this.executor = executor;
        this.scheduler = scheduler;
        grid = null;
        numCols = numRows = 0;
        isCoordinator = myNid == 0;

        numNodesHint = Short.parseShort(props.getProperty("numNodesHint", "" + numNodes));
        semAllJoined = semJoined;

        if (myAddr == null) {
            try {
                myCachedAddr = InetAddress.getLocalHost();
            } catch (UnknownHostException ex) {
                throw new RuntimeException(ex);
            }
        }
        else {
            myCachedAddr = myAddr;
        }

        myPort = basePort + myNid;

        clientTimeout = Integer.parseInt(props.getProperty("clientTimeout", "" + 2 * neighborBroadcastPeriod));
    }

    private final int myPort;

    private void handleInit(Init im) {
        if (im.id == -1) {
            throw new PlannedException("network is full; aborting");
        }
        System.out.println("Had nodeId = " + myNid + ". New nodeId = " + im.id);
        myNid = im.id;
        logger = Logger.getLogger("node_" + myNid);
        logger.addHandler(fh);
        currentStateVersion = im.version;
        log("got from coord => Init " + im.id);
        updateMembers(im.members);
    }

    private String bytes2string(byte[] buf) {
        String s = "[ ";
        for (byte b : buf) {
            s += b + " ";
        }
        s += "]";
        return s;
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

    public static final class PlannedException extends RuntimeException {
        public PlannedException(String msg) {
            super(msg);
        }
    }

    public final AtomicReference<Exception> failure = new AtomicReference<Exception>();
    public void run() {
        try {
            run2();
        } catch (PlannedException ex) {
            log(ex.getMessage());
            failure.set(ex);
            if (semAllJoined != null) semAllJoined.release();
        } catch (Exception ex) {
            err(ex);
            failure.set(ex);
            if (semAllJoined != null) semAllJoined.release();
        }
    }

    private short nextNodeId = 1;

    public void run2() {
        if (isCoordinator) {
            try {
                scheduler.scheduleAtFixedRate(safeRun(new Runnable() {
                    public void run() {
                        log("checkpoint: " + coordNodes.size() + " nodes");
                        printMembers();
                        //printGrid();
                    }
                }), dumpPeriod, dumpPeriod, TimeUnit.SECONDS);
                if (membershipBroadcastPeriod > 0) {
                    scheduler.scheduleAtFixedRate(safeRun(new Runnable() {
                        public void run() {
                            if (membersChanged.get()) {
                                broadcastMembershipChange((short) 0);
                            }
                        }
                    }), 1, membershipBroadcastPeriod, TimeUnit.SECONDS);
                }
                // do not remove this for now
                Thread.sleep(2000);
                new DatagramAcceptor().bind(new InetSocketAddress(InetAddress
                        .getLocalHost(), basePort), new CoordReceiver());
                ServerSocket ss = new ServerSocket(basePort);
                try {
                    ss.setReuseAddress(true);
                    ss.setSoTimeout(1000);
                    log("Beep!");

                    final Hashtable<Short, Socket> incomingSocks = new Hashtable<Short, Socket>();
                    while (!doQuit.get()) {
                        final Socket incoming;
                        try {
                            incoming = ss.accept();
                        } catch (SocketTimeoutException ex) {
                            continue;
                        }
                        final short nodeId;
                        // this is OK since nid orderings are irrelevant
                        synchronized (NeuRonNode.this) {
                            nodeId = nextNodeId++;
                        }

                        executor.submit(new Runnable() {
                            public void run() {
                                try {
                                    Join msg = (Join) new Serialization().deserialize(new DataInputStream(incoming.getInputStream()));

                                    synchronized (NeuRonNode.this) {
                                        incomingSocks.put(nodeId, incoming);
                                        if (!capJoins || coordNodes.size() < numNodesHint) {
                                            addMember(nodeId, msg.addr, msg.port, msg.src);
                                            if (coordNodes.size() == numNodesHint) {
                                                semAllJoined.release();
                                            }
                                            if (blockJoins) {
                                                if (coordNodes.size() >= numNodesHint) {
                                                    // time to broadcast ims to everyone
                                                    ArrayList<NodeInfo> memberList = getMemberInfos();
                                                    for (NodeInfo m : memberList) {
                                                        try {
                                                            doit(incomingSocks,
                                                                    memberList,
                                                                    m.id);
                                                        } finally {
                                                            incomingSocks.get(m.id).close();
                                                        }
                                                    }
                                                }
                                            } else {
                                                doit(incomingSocks, getMemberInfos(), nodeId);
                                                broadcastMembershipChange(nodeId);
                                            }
                                        } else if (capJoins && coordNodes.size() == numNodesHint) {
                                            Init im = new Init();
                                            im.src = myNid;
                                            im.id = -1;
                                            im.members = new ArrayList<NodeInfo>();
                                            sendit(incoming, im);
                                        }
                                    }
                                } catch (Exception ex) {
                                    err(ex);
                                } finally {
                                    try {
                                        if (!blockJoins) incoming.close();
                                    } catch (IOException ex) {
                                        err(ex);
                                    }
                                }
                            }

                            private void doit(
                                    final Hashtable<Short, Socket> incomingSocks,
                                    ArrayList<NodeInfo> memberList,
                                    short nid) throws IOException {
                                Init im = new Init();
                                im.id = nid;
                                im.src = myNid;
                                im.version = currentStateVersion;
                                im.members = memberList;
                                sendit(incomingSocks.get(nid), im);
                            }

                            private void sendit(
                                    Socket socket, Init im) throws IOException {
                                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                                new Serialization().serialize(im, dos);
                                dos.flush();
                            }
                        });
                    }
                } finally {
                    ss.close();
                    log("coord done");
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            Socket s = null;
            long startTime = System.currentTimeMillis();
            int count = 0;
            while (true) {
                try {
                    if (count++ > joinTimeLimit) {
                        throw new PlannedException("exceeded join try limit; aborting");
                    }
                    // connect to the coordinator
                    try {
                        s = new Socket(coordinatorHost, basePort);
                        break;
                    } catch (Exception ex) {
                        log("couldn't connect to coord, retrying in 1 sec: " + ex.getMessage());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                        }
                    }

                    try {
                        // talk to coordinator
                        log("sending join to coordinator at " + coordinatorHost + ":" + basePort);
                        Join msg = new Join();
                        msg.addr = myCachedAddr;
                        msg.src = myNid; // informs coord of orig id
                        msg.port = myPort;
                        DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                        new Serialization().serialize(msg, dos);
                        dos.flush();

                        log("waiting for InitMsg");
                        ByteArrayOutputStream minibaos = new ByteArrayOutputStream();
                        byte[] minibuf = new byte[8192];
                        int amt;
                        while ((amt = s.getInputStream().read(minibuf)) > 0) {
                            minibaos.write(minibuf, 0, amt);
                        }
                        byte[] buf = minibaos.toByteArray();
                        try {
                            Init im = (Init) new Serialization().deserialize(new DataInputStream(new ByteArrayInputStream(buf)));
                            handleInit(im);
                        } catch (Exception ex) {
                            err("got buffer: " + bytes2string(buf));
                            throw ex;
                        }
                    } finally {
                        try {
                            s.close();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                } catch (PlannedException ex) {
                    throw ex;
                } catch (SocketException ex) {
                    warn(ex.getMessage());
                    return;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            // wait for coordinator to announce my existence to others
            try {
                Thread.sleep(membershipBroadcastPeriod * 1000);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }

            // now start accepting pings and other msgs,
            // also start sending probes and sending out other msgs
            try {
                new DatagramAcceptor().bind(new InetSocketAddress(myCachedAddr, myPort),
                                            new Receiver());
                log("server started on " + myCachedAddr + ":" + (basePort + myNid));
                scheduler.scheduleAtFixedRate(safeRun(new Runnable() {
                    public void run() {
                        pingAll();
                    }
                }), 1, probePeriod, TimeUnit.SECONDS);
                scheduler.scheduleAtFixedRate(safeRun(new Runnable() {
                    public void run() {
                        ArrayList<NodeState> measRecips = scheme == RoutingScheme.SIMPLE ?
                                otherNodes : getAllRendezvousServers();
                        broadcastMeasurements(measRecips);
                        if (scheme != RoutingScheme.SIMPLE) {
                            broadcastRecommendations();
                        }
                    }
                }), 1, neighborBroadcastPeriod, TimeUnit.SECONDS);
                if (semAllJoined != null) semAllJoined.release();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private final HashSet<Short> ignored = new HashSet<Short>();

    public synchronized void ignore(short nid) {
        log("ignoring " + nid);
        ignored.add(nid);
    }

    public synchronized void unignore(short nid) {
        log("unignoring " + nid);
        ignored.remove(nid);
    }

    private void pingAll() {
        log("pinging");
        Ping ping = new Ping();
        ping.time = System.currentTimeMillis();
        NodeInfo tmp = nodes.get(myNid).info;
        ping.info = new NodeInfo();
        ping.info.id = origNid; // note that the ping info uses the original id
        ping.info.addr = tmp.addr;
        ping.info.port = tmp.port;
        for (short nid : nodes.keySet())
            if (nid != myNid)
                sendObject(ping, nid);

        /* send ping to the membership server (co-ord) -
           this might not be required if everone makes their own local decision.
           i.e. each node notices that no other node can reach a node (say X),
           then each node sends the co-ord a msg saying that "i think X is dead".
           The sending of this msg can be staggered in time so that the co-ord is not flooded with mesgs.
           The co-ordinator can then make a decision on keeping or removing node Y from the membership.
           On seeing a subsequent msg from the co-ord that X has been removed from the overlay, if a node Y
           has not sent its "i think X is dead" msg, it can cancel this event.
        */
        sendObject(ping, (short)0);
    }

    private Msg deserialize(Object o) {
        ByteBuffer buf = (ByteBuffer) o;
        byte[] bytes = new byte[buf.limit()];
        buf.get(bytes);
        try {
            return (Msg) new Serialization().deserialize(new DataInputStream(new
                        ByteArrayInputStream(bytes)));
        } catch (Exception ex) {
            err("deserialization exception: " + ex.getMessage());
            return null;
        }
    }

    private Hashtable<Short,Short> id2id = new Hashtable<Short,Short>();
    private Hashtable<Short,String> id2name = new Hashtable<Short,String>();

    /**
     * coordinator's msg handling loop
     */
    public final class CoordReceiver extends IoHandlerAdapter {
        @Override
        public void messageReceived(IoSession session, Object obj)
                throws Exception {
            try {
                Msg msg = deserialize(obj);
                if (msg == null) return;
                synchronized (NeuRonNode.this) {
                    if (msg.session == sessionId) {
                        if (coordNodes.containsKey(msg.src)) {
                            log("recv." + msg.getClass().getSimpleName(), "from " +
                                    msg.src + " (oid " + id2id.get(msg.src) + ", "
                                    + id2name.get(msg.src) + ")");
                            resetTimeoutAtCoord(msg.src);
                            if (msg.version < currentStateVersion) {
                                log("updating stale membership");
                                sendMembership(msg.src);
                            }
                            if (msg instanceof Ping) {
                                // ignore the ping
                            } else {
                                throw new Exception("can't handle that message type");
                            }
                        } else {
                            if ((!capJoins || coordNodes.size() < numNodesHint) &&
                                    msg instanceof Ping) {
                                Ping ping = (Ping) msg;
                                log("dead." + ping.getClass().getSimpleName(),
                                        "from '" + ping.src + "' " + ping.info.addr.getHostName());

                                Short mappedId = addr2id.get(ping.info.addr);
                                short nid;
                                if (mappedId == null) {
                                    nid = nextNodeId++;
                                    addMember(nid, ping.info.addr,
                                            ping.info.port, ping.info.id);
                                    broadcastMembershipChange(nid);
                                } else {
                                    nid = mappedId;
                                }

                                Init im = new Init();
                                im.id = nid;
                                im.src = myNid;
                                im.version = currentStateVersion;
                                im.members = getMemberInfos();
                                sendObject(im, nid);
                            } else {
                                log("dead." + msg.getClass().getSimpleName(), "from '" + msg.src + "'");
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                err(ex);
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
            try {
                Msg msg = deserialize(obj);
                if (msg == null) return;
                synchronized (NeuRonNode.this) {
                    if ((msg.src == 0 || nodes.containsKey(msg.src)) &&
                            msg.session == sessionId || msg instanceof Ping) {
                    	NodeState state = nodes.get(msg.src);

                        log("recv." + msg.getClass().getSimpleName(), "from " + msg.src);

                        // always reply to pings and log pongs

                        if (msg instanceof Ping) {
                            Ping ping = ((Ping) msg);
                            Pong pong = new Pong();
                            pong.time = ping.time;
                            sendObject(pong, ping.info);
                        } else if (msg instanceof Pong) {
                            Pong pong = (Pong) msg;
                            short rtt = (short) (System.currentTimeMillis() - pong.time);
                            
                            if (state != null) {
                                resetTimeoutAtNode(pong.src);
                                NodeState self = nodes.get(myNid);
                                short oldLatency = self.latencies.get(pong.src);
                                short ewma = (short) (smoothingFactor
                                        * (rtt / 2) + (1 - smoothingFactor)
                                        * oldLatency);
                                log("latency", pong.src + " = " + rtt/2 + ", ewma " + ewma);
                                self.latencies.put(pong.src, ewma);
                            } else {
                                log("latency", "some " + pong.src + " = " + rtt/2);
                            }
                        }

                        // for other messages, make sure their state version is
                        // the same as ours

                        if (msg.version > currentStateVersion) {
                            if (msg instanceof Membership) {
                                currentStateVersion = msg.version;
                                Membership m = (Membership) msg;
                                myNid = m.yourId;
                                updateMembers(m.members);
                            } else {
                                // i am out of date - request latest membership
                                // sendObject(new MemberPoll(), 0);
                                // commented out - membership updates now
                                // implicitly handled via pings
                            }
                        } else if (msg.version == currentStateVersion) {
                        	// from coordinator
                            if (msg instanceof Membership) {
                                Membership m = (Membership) msg;
                                myNid = m.yourId;
                                updateMembers(m.members);
                            } else if (msg instanceof Measurements) {
                                updateMeasurements((Measurements) msg);
                            } else if (msg instanceof RoutingRecs) {
                                RoutingRecs recs = (RoutingRecs) msg;
                                handleRecommendations(recs);
                                log("got recs " + routesToString(recs.recs)
                                        + ", " + countReachableNodes()
                                        + " total reachable nodes");

                                Pair<Integer, Integer> p = countAvaibleOneHopPaths();
                                log("total reachable nodes = " + p.first());
                                log("average available one-hop paths = " + p.second());
                            } else if (msg instanceof Ping) {
                                // nothing to do, already handled above
                            } else if (msg instanceof Pong) {
                                // nothing to do, already handled above
                            } else if (msg instanceof PeeringRequest) {
                                resetTimeoutOnRendezvousClient(msg.src);
                                rendezvousClients.add(nodes.get(msg.src));
                            } else if (msg instanceof Init) {
                                handleInit((Init) msg);
                            } else {
                                throw new Exception("can't handle that message type");
                            }
                        } else {
                            log("stale." + msg.getClass().getSimpleName(), "from " + msg.src + " version " + msg.version);
                        }
                    } else {
                        // log("ignored." + msg.getClass().getSimpleName(), "ignored from " + msg.src + " session " + msg.session);
                    }
                }
            } catch (Exception ex) {
                err(ex);
            }
        }
    }

    /**
     * If we don't hear from a node for this number of seconds, then consider
     * them dead.
     */
    private int membershipTimeout;
    private Hashtable<Short, ScheduledFuture<?>> timeouts = new Hashtable<Short, ScheduledFuture<?>>();

    /**
     * a coord-only method
     *
     * @param nid
     */
    private void resetTimeoutAtCoord(final short nid) {
        if (coordNodes.containsKey(nid)) {
            ScheduledFuture<?> oldFuture = timeouts.get(nid);
            if (oldFuture != null) {
                oldFuture.cancel(false);
            }
            ScheduledFuture<?> future = scheduler.schedule(safeRun(new Runnable() {
                public void run() {
                    removeMember(nid);
                }
            }), membershipTimeout, TimeUnit.SECONDS);
            timeouts.put(nid, future);
        }
    }

    private final int clientTimeout;

    private void resetTimeoutOnRendezvousClient(final short nid) {
        final NodeState node = nodes.get(nid);
        if (!node.isReachable) return;

        ScheduledFuture<?> oldFuture = timeouts.get(nid);
        if (oldFuture != null) {
            oldFuture.cancel(false);
        }

        ScheduledFuture<?> future = scheduler.schedule(safeRun(new Runnable() {
            public void run() {
                rendezvousClients.remove(scheduler);
            }
        }), clientTimeout, TimeUnit.SECONDS);
    }

    private void resetTimeoutAtNode(final short nid) {
        if (nodes.containsKey(nid)) {
            ScheduledFuture<?> oldFuture = timeouts.get(nid);
            if (oldFuture != null) {
                oldFuture.cancel(false);
            }
            final NodeState node = nodes.get(nid);
            if (!node.isReachable)
                log(nid + " reachable");
            node.isReachable = true;

            ScheduledFuture<?> future = scheduler.schedule(safeRun(new Runnable() {
                public void run() {
                    if (nodes.containsKey(nid)) {
                        log(nid + " unreachable");
                        node.isReachable = false;
                        nodes.get(myNid).latencies.remove(nid);

                        rendezvousClients.remove(node);
                        ArrayList<NodeState> clients = getAllRendezvousClients();

                        // if nid was someone's hop, fix that. note that this
                        // includes the node itself, which we want.
                        for (NodeState node : nodes.values()) {
                            if (node.hop == nid) {
                                if (node.isReachable) {
                                    node.hop = node.info.id;
                                } else {
                                    node.hop = 0;
                                    // see if a rendezvous client can serve as the hop.
                                    // TODO choose the lowest-latency route.
                                    short min = resetLatency;
                                    for (NodeState client : clients) {
                                        short val = client.latencies.get(nid);
                                        if (val < min) {
                                            node.hop = client.info.id;
                                            min = val;
                                        }
                                    }
                                    if (node.hop == 0) {
                                        log("node " + node + " down");
                                    }
                                }
                            }
                        }
                    }
                }
            }), linkTimeout, TimeUnit.SECONDS);
            timeouts.put(nid, future);
        }
    }

    /**
     * a coordinator-only method
     */
    private NodeInfo addMember(short newNid, InetAddress addr, int port, short origId) {
        NodeInfo info = new NodeInfo();
        info.id = newNid;
        info.addr = addr;
        info.port = port;
        coordNodes.put(newNid, info);
        id2id.put(newNid, origId);
        id2name.put(newNid, addr.getHostName());
        addr2id.put(addr, newNid);
        log("adding new node: " + newNid + " oid " + origId + " name " +
                id2name.get(newNid));
        currentStateVersion++;
        resetTimeoutAtCoord(newNid);
        return info;
    }

    private ArrayList<Short> getUncachedmemberNids() {
        ArrayList<Short> nids = new ArrayList<Short>(nodes.keySet());
        Collections.sort(nids);
        return nids;
    }

    private final AtomicBoolean membersChanged = new AtomicBoolean();

    /**
     * a coordinator-only method
     *
     * @param exceptNid - if this is 0, then we must have been called by the
     * periodic membership-broadcast daemon thread, so actually send stuff;
     * otherwise, we should just signal to the daemon thread a pending change
     */
    private void broadcastMembershipChange(short exceptNid) {
        if (exceptNid == 0 || membershipBroadcastPeriod == 0) {
            for (short nid : nodes.keySet()) {
                if (nid != exceptNid) {
                    sendMembership(nid);
                }
            }
        }
    }

    ArrayList<NodeInfo> getMemberInfos() {
        return new ArrayList<NodeInfo>(coordNodes.values());
    }

    /**
     * a coordinator-only method
     *
     * throttles these messages so they're sent at most once per second
     */
    private void sendMembership(short nid) {
        Membership msg = new Membership();
        msg.yourId = nid;
        msg.members = getMemberInfos();
        sendObject(msg, nid);
    }

    /**
     * a coordinator-only method
     *
     * @param nid
     */
    private void removeMember(short nid) {
        log("removing dead node " + nid + " oid " + id2id.get(nid) + " " +
                id2name.get(nid));
        NodeInfo info = coordNodes.remove(nid);
        Short mid = addr2id.remove(info.addr);
        assert mid != null;
        currentStateVersion++;
        broadcastMembershipChange(nid);
    }

    /**
     * updates our member state. modifies data structures as necessary to
     * maintain invariants.
     *
     * @param newNodes
     */
    private void updateMembers(List<NodeInfo> newNodes) {

        // add new nodes

        for (NodeInfo node : newNodes)
            if (!nodes.containsKey(node.id))
                nodes.put(node.id, new NodeState(node));

        // remove nodes

        HashSet<Short> newNids = new HashSet<Short>();
        for (NodeInfo node : newNodes)
            newNids.add(node.id);
        HashSet<Short> toRemove = new HashSet<Short>();
        for (Short nid : nodes.keySet())
            if (!newNids.contains(nid))
                toRemove.add(nid);
        for (Short nid : toRemove)
            nodes.remove(nid);

        // consistency cleanups: check that all nid references are still valid nid's

        for (NodeState state : nodes.values()) {
            if (!newNids.contains(state.hop))
                state.hop = state.info.id;

            for (Iterator<Short> i = state.hopOptions.iterator(); i.hasNext();)
                if (!newNids.contains(i.next()))
                    i.remove();

            HashSet<Short> garbage = new HashSet<Short>();
            for (short nid : state.latencies.keySet())
                if (!newNids.contains(nid))
                    garbage.add(nid);
            for (short nid : garbage)
                state.latencies.remove(nid);
        }

        //
        // regenerate alternative views of this data
        //

        NodeState self = nodes.get(myNid);

        memberNids.clear();
        memberNids.addAll(newNids);
        Collections.sort(memberNids);

        otherNodes.clear();
        otherNodes.addAll(nodes.values());
        otherNodes.remove(self);

        numCols = (short) Math.ceil(Math.sqrt(nodes.size()));
        numRows = (short) Math.ceil((double) nodes.size() / (double) numCols);
        grid = new NodeState[numRows][numCols];
        List<Short> nids = memberNids;
        for (short i = 0, r = 0; r < numRows; r++)
            for (short c = 0; c < numCols; c++)
                grid[r][c] = nodes.get(nids.get(i++ % nids.size()));

        /*
         * simply forget about all our neighbors. thus, this forgets all our
         * failover clients and servers. since the grid is different. if this
         * somehow disrupts route computation, so be it - it'll only last for a
         * period.
         *
         * one worry is that others who miss this member update will continue to
         * broadcast to us. this is a non-issue because we ignore stale
         * messages, and when they do become updated, they'll forget about us
         * too.
         */
        rendezvousClients.clear();
        defaultRendezvousServers.clear();
        for (int rz = 0; rz < numRows; rz++) {
            for (int cz = 0; cz < numCols; cz++) {
                if (grid[rz][cz] == self) {
                    HashSet<NodeState> rendezvousClientRow = new HashSet<NodeState>();
                    HashSet<NodeState> rendezvousClientCol = new HashSet<NodeState>();
                    // add this column and row as clients
                    for (int r1 = 0; r1 < numRows; r1++) {
                        NodeState cli = grid[r1][cz];
                        if (cli.isReachable && cli != self)
                            rendezvousClientCol.add(cli);
                    }
                    for (int c1 = 0; c1 < numCols; c1++) {
                        NodeState cli = grid[rz][c1];
                        if (cli.isReachable && cli != self)
                            rendezvousClientRow.add(cli);
                    }
                    rendezvousClients.addAll(rendezvousClientRow);
                    rendezvousClients.addAll(rendezvousClientCol);
                    
                    // add the rendezvous servers to all nodes
                    for (int r0 = 0; r0 < numRows; r0++) {
                        for (int c0 = 0; c0 < numCols; c0++) {
                            NodeState dst = grid[r0][c0];
                            HashSet<NodeState> rs = defaultRendezvousServers.get(dst.info.id);
                            if (rs == null) {
                                rs = new HashSet<NodeState>();
                                defaultRendezvousServers.put(dst.info.id, rs);
                            }
                            if (r0 != rz && c0 != cz) {
                                // normally, add the pairs
                                if (self != grid[rz][c0])
                                    rs.add(grid[rz][c0]);
                                if (self != grid[r0][cz])
                                    rs.add(grid[r0][cz]);
                            } else if (c0 == cz) {
                                /*
                                 * if this is in our col (a neighbor), everyone
                                 * else in that col is in essence a rendezvous
                                 * server between us two
                                 */
                                rs.addAll(rendezvousClientCol);
                            } else if (r0 == rz) {
                                /*
                                 * ditto for rows
                                 */
                                rs.addAll(rendezvousClientRow);
                            }
                        }
                    }
                }
            }
        }
        rendezvousServers.clear();
        for (Entry<Short, HashSet<NodeState>> entry : defaultRendezvousServers.entrySet()) {
            rendezvousServers.put(entry.getKey(), new HashSet<NodeState>());
        }

        log("state " + currentStateVersion + ", mbrs " + nids);
    }

    /**
     * @param n
     * @param remoteNid
     * @return
     */
    private boolean isFailedRendezvous(NodeState n, short remoteNid) {
        return !n.isReachable || n.remoteFailures.contains(remoteNid);
    }

    /**
     * @return failoverClients `union` nodes in my row and col (wherever i occur)
     */
    private ArrayList<NodeState> getAllRendezvousClients() {
        ArrayList<NodeState> list = new ArrayList<NodeState>(rendezvousClients);
        Collections.sort(list);
        return list;
    }

    /**
     * makes one pass over the metaset of all rendezvous servers, removing any
     * failed rendezvous from the individual sets.
     *
     * for the simple routing scheme, this is the full set of nodes. as a
     * result, measurements are broadcast to everyone, as intended. (note that
     * there are no routing recommendation messages in this scheme.)
     *
     * @return the union of all the sets of non-failed rendezvous servers.
     */
    private ArrayList<NodeState> getAllRendezvousServers() {
        HashSet<NodeState> servers = new HashSet<NodeState>();
        NodeState self = nodes.get(myNid);
        for (int r0 = 0; r0 < numRows; r0++) {
            for (int c0 = 0; c0 < numCols; c0++) {
                NodeState dst = grid[r0][c0];

                // if dst is not us and we believe that the node is not down
                if (dst != self && dst.hop != 0) {
                    HashSet<NodeState> rs = rendezvousServers.get(dst.info.id);

                    // check if any of our default rendezvous servers are once
                    // more available; if so, add them back
                    HashSet<NodeState> defaults = defaultRendezvousServers.get(dst.info.id);
                    boolean cleared = false;
                    for (NodeState r : defaults) {
                        if (!isFailedRendezvous(r, dst.info.id)) {
                            if (!cleared) {
                                rs.clear();
                                cleared = true;
                            }
                            rs.add(r);
                        }
                    }

                    if (rs.isEmpty()) {
                        // look for failovers

                        // get candidates from col
                        ArrayList<NodeState> cands = new ArrayList<NodeState>();
                        for (int r1 = 0; r1 < numRows; r1++) {
                            NodeState cand = grid[r1][c0];
                            if (cand != self && cand.isReachable)
                                cands.add(cand);
                        }

                        // get candidates from row
                        for (int c1 = 0; c1 < numCols; c1++) {
                            NodeState cand = grid[r0][c1];
                            if (cand != self && cand.isReachable)
                                cands.add(cand);
                        }

                        // choose candidate uniformly at random
                        NodeState failover = cands.get(rand.nextInt(cands.size()));
                        log("new failover for " + dst + ": " + failover);
                        rs.add(failover);
                        servers.add(failover);
                    } else {
                        /*
                         * when we remove nodes now, don't immediately look
                         * for failovers. the next period, we will have
                         * received link states from our neighbors, from
                         * which we can determine whether dst is just down.
                         */
                        for (Iterator<NodeState> i = rs.iterator(); i.hasNext();) {
                            NodeState r = i.next();
                            if (isFailedRendezvous(r, dst.info.id)) {
                                i.remove();
                            } else {
                                servers.add(r);
                            }
                        }
                    }
                }
            }
        }
        ArrayList<NodeState> list = new ArrayList<NodeState>(servers);
        Collections.sort(list);
        return list;
    }


    public static enum RoutingScheme { SIMPLE, SQRT, SQRT_NOFAILOVER, SQRT_RC_FAILOVER, SQRT_SPECIAL };
    private final RoutingScheme scheme;

    private void printMembers() {
        String s = "members:";
        for (NodeInfo info : coordNodes.values()) {
            s += "\n  " + info.id + " oid " + id2id.get(info.id) + " " +
                id2name.get(info.id) + " " + info.port;
        }
        log(s);
    }

    // PERF
    private void printGrid() {
        String s = "grid:";
        if (grid != null) {
            for (int i = 0; i < numRows; i++) {
                s += "\n  ";
                for (int j = 0; j < numCols; j++) {
                    s += "\t" + grid[i][j];
                }
            }
        }
        log(s);
    }

    /**
     * in the sqrt routing scheme: for each neighbor, find for him the min-cost
     * hops to all other neighbors, and send this info to him (the intermediate
     * node may be one of the endpoints, meaning a direct route is cheapest).
     *
     * in the sqrt_special routing scheme, we instead find for each neighbor the
     * best intermediate other neighbor through which to route to every
     * destination. this still needs work, see various todos.
     *
     * a failed rendezvous wrt some node n is one which we cannot reach
     * (proximal failure) or which cannot reach n (remote failure). when all
     * current rendezvous to some node n fail, then we find a failover from node
     * n's row and col, and include it in our neighbor set. by befault, this
     * situation occurs when a row-col rendezvous pair fail. it can also occur
     * with any of our current failovers.
     */
    private void broadcastRecommendations() {
        ArrayList<NodeState> clients = getAllRendezvousClients();
        @SuppressWarnings("unchecked")
        ArrayList<NodeState> dsts = (ArrayList<NodeState>) clients.clone();
        dsts.add(nodes.get(myNid));
        Collections.sort(dsts);
        int totalSize = 0;
        for (NodeState src : clients) {
            ArrayList<Rec> recs = new ArrayList<Rec>();

            // dst <- nbrs, hop <- any
            findHops(dsts, memberNids, src, recs);

            /*
             * TODO: need to additionally send back info about *how good* the
             * best hop is, so that the receiver can decide which of the many
             * recommendations to accept
             */
            if (scheme == RoutingScheme.SQRT_SPECIAL) {
                // dst <- any, hop <- nbrs
                findHopsAlt(memberNids, dsts, src, recs);
            }

            RoutingRecs msg = new RoutingRecs();
            msg.recs = recs;
            totalSize += sendObject(msg, src.info.id);
        }
        log("sent recs, " + totalSize + " bytes, to " + clients);
    }

    private void findHops(ArrayList<NodeState> dsts,
            ArrayList<Short> hops, NodeState src, ArrayList<Rec> recs) {
        for (NodeState dst : dsts) {
            if (src != dst) {
                short min = resetLatency;
                short minhop = -1;
                for (short hop : hops) {
                    if (hop != src.info.id) {
                        short src2hop = src.latencies.get(hop);
                        short dst2hop = dst.latencies.get(hop);
                        short latency = (short) (src2hop + dst2hop);
                        if (latency < min) {
                            min = latency;
                            minhop = hop;
                        }
                    }
                }
                assert minhop != -1;
                Rec rec = new Rec();
                rec.dst = dst.info.id;
                rec.via = minhop;
                recs.add(rec);
            }
        }
    }

    private void findHopsAlt(ArrayList<Short> dsts,
            ArrayList<NodeState> hops, NodeState src, ArrayList<Rec> recs) {
        for (short dst : dsts) {
            if (src.info.id != dst && nodes.get(dst).isReachable) {
                short min = resetLatency;
                short minhop = -1;
                for (NodeState hop : hops) {
                    if (hop != src) {
                        short src2hop = src.latencies.get(hop.info.id);
                        short dst2hop = hop.latencies.get(dst);
                        short latency = (short) (src2hop + dst2hop);
                        if (latency < min) {
                            min = latency;
                            minhop = hop.info.id;
                        }
                    }
                }
                assert minhop != -1;
                Rec rec = new Rec();
                rec.dst = dst;
                rec.via = minhop;
                recs.add(rec);
            }
        }
    }

    private String routesToString(ArrayList<Rec> recs) {
        String s = "";
        for (Rec rec : recs)
            s += rec.via + "->" + rec.dst + " ";
        return s;
    }

    private Serialization senderSer = new Serialization();

    private int sendObject(final Msg o, InetAddress addr, int port, short nid) {
        o.src = myNid;
        o.version = currentStateVersion;
        o.session = sessionId;

        try {
            /*
             * note that it's unsafe to re-use these output streams - at
             * least, i don't know how (reset() is insufficient)
             */
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            senderSer.serialize(o, new DataOutputStream(baos));
            byte[] buf = baos.toByteArray();
            String who = nid >= 0 ? "" + nid : (addr + ":" + port);
            log("send." + o.getClass().getSimpleName(),
                    "to " + who + " len " + buf.length);
            if (!ignored.contains(nid)) {
                sendSocket.send(new DatagramPacket(buf, buf.length, addr, port));
            } else {
                log("droppng packet sent to " + who);
            }
            return buf.length;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private int sendObject(final Msg o, NodeInfo info, short nid) {
        return sendObject(o, info.addr, info.port, nid);
    }

    private int sendObject(final Msg o, NodeInfo info) {
        return sendObject(o, info, (short)-1);
    }

    private int sendObject(final Msg o, short nid) {
        return nid != myNid ?
            sendObject(o, nid == 0 ? coordNode : nodes.get(nid).info, nid) : 0;
    }

    private void broadcastMeasurements(ArrayList<NodeState> servers) {
        ShortShortMap latencies = nodes.get(myNid).latencies;

        Measurements rm = new Measurements();
        rm.probeTable = new short[memberNids.size()];
        for (int i = 0; i < rm.probeTable.length; i++)
            rm.probeTable[i] = latencies.get(memberNids.get(i));
        rm.inflation = new byte[rm.probeTable.length];

        int totalSize = 0;
        for (NodeState nbr : servers) {
            totalSize += sendObject(rm, nbr.info.id);
        }
        log("sent measurements, " + totalSize + " bytes, to " + servers);
    }

    private void updateMeasurements(Measurements m) {
        NodeState myState = nodes.get(m.src);
        for (int i = 0; i < m.probeTable.length; i++)
            if (m.probeTable[i] != resetLatency)
                myState.latencies.put(memberNids.get(i), m.probeTable[i]);
    }

    private void handleRecommendations(RoutingRecs msg) {
        ArrayList<Rec> recs = msg.recs;
        HashSet<Short> dstsPresent = new HashSet<Short>();
        for (Rec r : recs) {
            if (nodes.get(r.via).isReachable) {
                if (scheme == RoutingScheme.SQRT_SPECIAL) {
                    /*
                     * TODO: add in support for processing sqrt_special
                     * recommendations. first we need to add in the actual cost of
                     * the route to these recommendations (see
                     * broadcastRecommndations), then we need to compare all of
                     * these and see which ones were better. a complication is that
                     * routing recommendation broadcasts are not synchronized, so
                     * while older messages may appear to have better routes, there
                     * must be some threshold in time past which we disregard old
                     * latencies. must keep some history
                     */
                    nodes.get(r.dst).hopOptions.add(r.via);
                    nodes.get(r.dst).hop = r.via;
                } else {
                    // blindly trust the recommendations
                    nodes.get(r.dst).hop = r.via;
                    dstsPresent.add(r.dst);
                }
            }
        }

        if (scheme != RoutingScheme.SQRT_SPECIAL) {
            /*
             * get the full set of dsts that we depend on this node for. note
             * that it may be serving a different set of nodes.
             */
            HashSet<NodeState> dsts = new HashSet<NodeState>();
            for (int r0 = 0; r0 < numRows; r0++) {
                for (int c0 = 0; c0 < numCols; c0++) {
                    if (msg.src == grid[r0][c0].info.id) {
                        for (int r1 = 0; r1 < numRows; r1++)
                            dsts.add(grid[r1][c0]);
                        for (int c1 = 0; c1 < numCols; c1++)
                            dsts.add(grid[r0][c1]);
                    }
                }
            }

            NodeState r = nodes.get(msg.src);
            r.remoteFailures.clear();
            for (NodeState dst : dsts) {
                if (!dstsPresent.contains(dst.info.id)) {
                    /*
                     * there was a comm failure between this rendezvous and the
                     * dst for which this rendezvous did not provide a
                     * recommendation. consider this a rendezvous failure, so that if
                     * necessary during the next phase, we will find failovers.
                     */
                    r.remoteFailures.add(dst);
                }
            }
        }
    }

    /**
     * TODO counts the number of nodes for which we have rendezvous
     * @return
     */
    private int countOptimalNodes() {
        return -1;
    }

    /**
     * counts the number of nodes that we can reach - either directly, through a
     * hop, or through any rendezvous client.
     *
     * @return
     */
    private int countReachableNodes() {
        /*
         * TODO need to fix up hopOptions so that it actually gets updated
         * correctly, since currently things are *never* removed from it (they
         * need to expire)
         */

        NodeState myState = nodes.get(myNid);
        int count = 0;
        for (NodeState node : otherNodes) {
            count += node.hop != 0 ? 1 : 0;
        }
        return count;
    }

    /**
     * counts the avg number of one-hop or direct paths available to nodes
     * @return
     */
    private Pair<Integer, Integer> countAvaibleOneHopPaths() {

        NodeState myState = nodes.get(myNid);
        int count = 0;
        int numNodesReachable = 0;
        for (NodeState node : otherNodes) {
        	HashSet<Short> availableHops = new HashSet<Short>();

        	if (node.hop != 0)
        		availableHops.add(node.hop);

        	if ((node.hop != myNid) && node.isReachable){
        		availableHops.add(myNid);
        	}

        	// for the available hop options that are valid (i.e. no proximal or remote failures)
        	for (Short hop : node.hopOptions) {
        		NodeState hopNodeState = nodes.get(hop);
        		if ((hopNodeState != null) && !isFailedRendezvous(hopNodeState, node.info.id)) {
            		availableHops.add(hop);
        		} else {
        			// TODO :: maybe we can fix node.hopOptions here!
        		}
        	}

            ArrayList<NodeState> clients = getAllRendezvousClients();
            for (NodeState client : clients) {
                if (client.latencies.containsKey(node.info.id)) {
            		availableHops.add(client.info.id);
                }
            }

            count += availableHops.size();
            if (!availableHops.isEmpty())
            	numNodesReachable++;
        }

        if (numNodesReachable != 0) {
        	count /= numNodesReachable;
        }
        return new Pair<Integer, Integer>(numNodesReachable, count);
    }

    public void quit() {
        doQuit.set(true);
    }

    private class NodeState implements Comparable<NodeState> {
        public String toString() {
            return "" + info.id;
        }
        /**
         * not null
         */
        public final NodeInfo info;

        /**
         * updated in resetTimeoutAtNode(). if hop == 0, this must be false; if
         * hop == the nid, this must be true.
         *
         * this should also be made to correspond with the appropriate latencies in myNid
         */
        public boolean isReachable = true;

        /**
         * the last known latencies to all other nodes. missing entry implies
         * resetLatency. this is populated/valid for rendezvous clients.
         *
         * invariants:
         *  - keyset is a subset of current members (memberNids); enforced in
         *    updateMembers()
         *  - keyset contains only live nodes; enforced in resetTimeoutAtNode()
         *  - values are not resetLatency
         *  - undefined if not a rendezvous client
         */

        public final ShortShortMap latencies = new ShortShortMap(resetLatency);

        /**
         * the recommended intermediate hop for us to get to this node, or 0 if
         * no way we know of to get to that node, and thus believe the node is
         * gone.
         *
         * invariants:
         *  - always refers to a member or 0; enforced in
         *    updateMembers()
         *  - never refers to dead node; enforced in resetTimeoutAtNode()
         *  - may refer to the node of this nodestate (may be dst)
         *  - never refers to the owning neuronnode (never is src)
         *  - cannot be the nid if !isReachable
         */

        public short hop;

        /**
         * remote failures. applies only if this nodestate is of a rendezvous
         * node. contains nids of all nodes for which this rendezvous cannot
         * recommend routes.
         *
         * invariants:
         *  - undefined if this is not a rendezvous node
         *  - empty
         */
        public final HashSet<NodeState> remoteFailures = new HashSet<NodeState>();

        /**
         * this is unused at the moment. still need to re-design.
         */

        public final HashSet<Short> hopOptions = new HashSet<Short>();

        public NodeState(NodeInfo info) {
            this.info = info;
        }

        public int compareTo(NodeState o) {
            return new Short(info.id).compareTo(o.info.id);
        }
    }
}


class ShortShortMap {
    private final Hashtable<Short,Short> table = new Hashtable<Short, Short>();
    private final short defaultValue;
    public ShortShortMap(short defaultValue) {
        this.defaultValue = defaultValue;
    }
    public Set<Short> keySet() {
        return table.keySet();
    }
    public boolean containsKey(short key) {
        return table.containsKey(key);
    }
    public void remove(short key) {
        table.remove(key);
    }
    public short get(short key) {
        Short value = table.get(key);
        return value != null ? value : defaultValue;
    }
    public void put(short key, short value) {
        if (value == defaultValue)
            table.remove(key);
        else
            table.put(key, value);
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

















class NodeInfo {
	short id;

	int port;

	InetAddress addr;
}

class Rec {
	short dst;

	short via;
}

class Msg {
	short src;

	short version;

	short session;
}

class Join extends Msg {
	InetAddress addr;

	int port;
}

class Init extends Msg {
	short id;

	ArrayList<NodeInfo> members;
}

class Membership extends Msg {
	ArrayList<NodeInfo> members;

	short numNodes;

	short yourId;
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
	short[] probeTable;

	byte[] inflation;
}

class MemberPoll extends Msg {
}

class PeeringRequest extends Msg {
}

class Serialization {

	public void serialize(Object obj, DataOutputStream out) throws IOException {
		if (false) {
		}

		else if (obj.getClass() == NodeInfo.class) {
			NodeInfo casted = (NodeInfo) obj;
			out.writeInt(0);
			out.writeShort(casted.id);
			out.writeInt(casted.port);
			byte[] buf = casted.addr.getAddress();
			out.writeInt(buf.length);
			out.write(buf);
		} else if (obj.getClass() == Rec.class) {
			Rec casted = (Rec) obj;
			out.writeInt(1);
			out.writeShort(casted.dst);
			out.writeShort(casted.via);
		} else if (obj.getClass() == Msg.class) {
			Msg casted = (Msg) obj;
			out.writeInt(2);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == Join.class) {
			Join casted = (Join) obj;
			out.writeInt(3);
			byte[] buf = casted.addr.getAddress();
			out.writeInt(buf.length);
			out.write(buf);
			out.writeInt(casted.port);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == Init.class) {
			Init casted = (Init) obj;
			out.writeInt(4);
			out.writeShort(casted.id);
			out.writeInt(casted.members.size());
			for (int i = 0; i < casted.members.size(); i++) {
				out.writeShort(casted.members.get(i).id);
				out.writeInt(casted.members.get(i).port);
				byte[] buf = casted.members.get(i).addr.getAddress();
				out.writeInt(buf.length);
				out.write(buf);
			}
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == Membership.class) {
			Membership casted = (Membership) obj;
			out.writeInt(5);
			out.writeInt(casted.members.size());
			for (int i = 0; i < casted.members.size(); i++) {
				out.writeShort(casted.members.get(i).id);
				out.writeInt(casted.members.get(i).port);
				byte[] buf = casted.members.get(i).addr.getAddress();
				out.writeInt(buf.length);
				out.write(buf);
			}
			out.writeShort(casted.numNodes);
			out.writeShort(casted.yourId);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == RoutingRecs.class) {
			RoutingRecs casted = (RoutingRecs) obj;
			out.writeInt(6);
			out.writeInt(casted.recs.size());
			for (int i = 0; i < casted.recs.size(); i++) {
				out.writeShort(casted.recs.get(i).dst);
				out.writeShort(casted.recs.get(i).via);
			}
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == Ping.class) {
			Ping casted = (Ping) obj;
			out.writeInt(7);
			out.writeLong(casted.time);
			out.writeShort(casted.info.id);
			out.writeInt(casted.info.port);
			byte[] buf = casted.info.addr.getAddress();
			out.writeInt(buf.length);
			out.write(buf);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == Pong.class) {
			Pong casted = (Pong) obj;
			out.writeInt(8);
			out.writeLong(casted.time);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == Measurements.class) {
			Measurements casted = (Measurements) obj;
			out.writeInt(9);
			out.writeInt(casted.probeTable.length);
			for (int i = 0; i < casted.probeTable.length; i++) {
				out.writeShort(casted.probeTable[i]);
			}
			out.writeInt(casted.inflation.length);
			out.write(casted.inflation);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == MemberPoll.class) {
			MemberPoll casted = (MemberPoll) obj;
			out.writeInt(10);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		} else if (obj.getClass() == PeeringRequest.class) {
			PeeringRequest casted = (PeeringRequest) obj;
			out.writeInt(11);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		}
	}

	public Object deserialize(DataInputStream in) throws IOException {
		switch (readInt(in)) {

		case 0: { // NodeInfo
			NodeInfo obj;
			{
				obj = new NodeInfo();
				{
					obj.id = in.readShort();
				}
				{
					obj.port = readInt(in);
				}
				{
					byte[] buf;
					{

						buf = new byte[readInt(in)];
						in.read(buf);

					}

					obj.addr = InetAddress.getByAddress(buf);

				}
			}
			return obj;
		}
		case 1: { // Rec
			Rec obj;
			{
				obj = new Rec();
				{
					obj.dst = in.readShort();
				}
				{
					obj.via = in.readShort();
				}
			}
			return obj;
		}
		case 2: { // Msg
			Msg obj;
			{
				obj = new Msg();
				{
					obj.src = in.readShort();
				}
				{
					obj.version = in.readShort();
				}
				{
					obj.session = in.readShort();
				}
			}
			return obj;
		}
		case 3: { // Join
			Join obj;
			{
				obj = new Join();
				{
					byte[] buf;
					{

						buf = new byte[readInt(in)];
						in.read(buf);

					}

					obj.addr = InetAddress.getByAddress(buf);

				}
				{
					obj.port = readInt(in);
				}
				{
					{
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}
		case 4: { // Init
			Init obj;
			{
				obj = new Init();
				{
					obj.id = in.readShort();
				}
				{
					obj.members = new ArrayList<NodeInfo>();
					for (int i = 0, len = readInt(in); i < len; i++) {
						NodeInfo x;
						{
							x = new NodeInfo();
							{
								x.id = in.readShort();
							}
							{
								x.port = readInt(in);
							}
							{
								byte[] buf;
								{

									buf = new byte[readInt(in)];
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
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}
		case 5: { // Membership
			Membership obj;
			{
				obj = new Membership();
				{
					obj.members = new ArrayList<NodeInfo>();
					for (int i = 0, len = readInt(in); i < len; i++) {
						NodeInfo x;
						{
							x = new NodeInfo();
							{
								x.id = in.readShort();
							}
							{
								x.port = readInt(in);
							}
							{
								byte[] buf;
								{

									buf = new byte[readInt(in)];
									in.read(buf);

								}

								x.addr = InetAddress.getByAddress(buf);

							}
						}
						obj.members.add(x);
					}
				}
				{
					obj.numNodes = in.readShort();
				}
				{
					obj.yourId = in.readShort();
				}
				{
					{
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}
		case 6: { // RoutingRecs
			RoutingRecs obj;
			{
				obj = new RoutingRecs();
				{
					obj.recs = new ArrayList<Rec>();
					for (int i = 0, len = readInt(in); i < len; i++) {
						Rec x;
						{
							x = new Rec();
							{
								x.dst = in.readShort();
							}
							{
								x.via = in.readShort();
							}
						}
						obj.recs.add(x);
					}
				}
				{
					{
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}
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
						obj.info.id = in.readShort();
					}
					{
						obj.info.port = readInt(in);
					}
					{
						byte[] buf;
						{

							buf = new byte[readInt(in)];
							in.read(buf);

						}

						obj.info.addr = InetAddress.getByAddress(buf);

					}
				}
				{
					{
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}
		case 8: { // Pong
			Pong obj;
			{
				obj = new Pong();
				{
					obj.time = in.readLong();
				}
				{
					{
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}
		case 9: { // Measurements
			Measurements obj;
			{
				obj = new Measurements();
				{
					obj.probeTable = new short[readInt(in)];
					for (int i = 0; i < obj.probeTable.length; i++) {
						{
							obj.probeTable[i] = in.readShort();
						}
					}
				}
				{

					obj.inflation = new byte[readInt(in)];
					in.read(obj.inflation);

				}
				{
					{
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}
		case 10: { // MemberPoll
			MemberPoll obj;
			{
				obj = new MemberPoll();
				{
					{
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}
		case 11: { // PeeringRequest
			PeeringRequest obj;
			{
				obj = new PeeringRequest();
				{
					{
						obj.src = in.readShort();
					}
					{
						obj.version = in.readShort();
					}
					{
						obj.session = in.readShort();
					}
				}
			}
			return obj;
		}

		default:
			throw new RuntimeException("unknown obj type");
		}
	}

	private byte[] readBuffer = new byte[4];

	public int readInt(DataInputStream dis) throws IOException {
		dis.readFully(readBuffer, 0, 4);
		return (((int) (readBuffer[0] & 255) << 24)
				+ ((readBuffer[1] & 255) << 16) + ((readBuffer[2] & 255) << 8) + ((readBuffer[3] & 255) << 0));
	}

	/*
	 * public static void main(String[] args) throws IOException { {
	 * ByteArrayOutputStream baos = new ByteArrayOutputStream();
	 * DataOutputStream out = new DataOutputStream(baos); Pong pong = new
	 * Pong(); pong.src = 2; pong.version = 3; pong.time = 4; serialize(pong,
	 * out); byte[] buf = baos.toByteArray(); System.out.println(buf.length);
	 * Object obj = deserialize(new DataInputStream(new
	 * ByteArrayInputStream(buf))); System.out.println(obj); }
	 *  { ByteArrayOutputStream baos = new ByteArrayOutputStream();
	 * DataOutputStream out = new DataOutputStream(baos);
	 *
	 * Measurements m = new Measurements(); m.src = 2; m.version = 3;
	 * m.membershipList = new ArrayList<Integer>(); m.membershipList.add(4);
	 * m.membershipList.add(5); m.membershipList.add(6); m.ProbeTable = new
	 * long[5]; m.ProbeTable[1] = 7; m.ProbeTable[2] = 8; m.ProbeTable[3] = 9;
	 *
	 * serialize(m, out); byte[] buf = baos.toByteArray();
	 * System.out.println(buf.length); Object obj = deserialize(new
	 * DataInputStream(new ByteArrayInputStream(buf))); System.out.println(obj); } {
	 * ByteArrayOutputStream baos = new ByteArrayOutputStream();
	 * DataOutputStream out = new DataOutputStream(baos);
	 *
	 * Membership m = new Membership(); m.src = 2; m.version = 3; m.members =
	 * new ArrayList<NodeInfo>(); NodeInfo n1 = new NodeInfo(); n1.addr =
	 * InetAddress.getLocalHost(); n1.port = 4; n1.id = 5; m.members.add(n1);
	 * NodeInfo n2 = new NodeInfo(); n2.addr =
	 * InetAddress.getByName("google.com"); n2.port = 6; n2.id = 7;
	 * m.members.add(n2); m.numNodes = 8;
	 *
	 * serialize(m, out); byte[] buf = baos.toByteArray();
	 * System.out.println(buf.length); Object obj = deserialize(new
	 * DataInputStream( new ByteArrayInputStream(buf)));
	 * System.out.println(obj); } }
	 */
}
