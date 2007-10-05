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

    private final Hashtable<Short, NodeInfo> nodes = new Hashtable<Short, NodeInfo>();

    // probeTable[i] = node members[i]'s probe table. value
    // at index j in row i is the link latency between nodes members[i]->members[j].
    short[][] probeTable;
    private GridNode[][] grid;
    private short numCols, numRows;
    private final HashSet<GridNode> overflowNeighbors = new HashSet<GridNode>();
    private Hashtable<Short, Short> nextHopTable = new Hashtable<Short, Short>();
    private Hashtable<Short, HashSet<Short>> nextHopOptions = new Hashtable<Short, HashSet<Short>>();
    private final IoServiceConfig cfg = new DatagramAcceptorConfig();

    private final Hashtable<InetAddress, Short> addr2id = new Hashtable<InetAddress, Short>();

    private short currentStateVersion;

    public final int neighborBroadcastPeriod;
    public final int probePeriod;

    private final NodeInfo coordNode;
    private final DatagramSocket sendSocket;

    private final RunMode mode;
    private final short numNodesHint;
    private final Semaphore semAllJoined;

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
    private final int failoverTimeout;

    private final int membershipBroadcastPeriod;

    private static final String defaultLabelSet = "send.Ping recv.Ping stale.Ping send.Pong recv.Pong stale.Pong send.Measurement send.RoutingRecs";

    private final Hashtable<Short,Long> lastSentMbr = new Hashtable<Short,Long>();

    private final double smoothingFactor;

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
        neighborBroadcastPeriod = Integer.parseInt(props.getProperty("neighborBroadcastPeriod", "10"));

        // for simulations we can safely reduce the probing frequency, or even turn it off
        if (mode == RunMode.SIM) {
            probePeriod = Integer.parseInt(props.getProperty("probePeriod", "60"));
        } else {
            probePeriod = Integer.parseInt(props.getProperty("probePeriod", "10"));
        }
        timeout = Integer.parseInt(props.getProperty("timeout", "" + probePeriod * 3));
        failoverTimeout = Integer.parseInt(props.getProperty("failoverTimeout", "" + timeout));
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
        probeTable = null;
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
        // the node id has to be logged here because
        // the node id received in the constructor (and passed to Logger) is different
        // from that in the InitMsg
        // This is the correct node ID !!!
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


    //private Runnable makeSafeRunnable(Runnable r) {
    //    return new Runnable() {
    //        public void run() {
    //            try {
    //                run();
    //            } catch (Exception ex) {
    //                err(ex);
    //            }
    //        }
    //    };
    //}

    public void run2() {
        if (isCoordinator) {
            try {
                scheduler.scheduleAtFixedRate(new Runnable() {
                    public void run() {
                        try {
                            synchronized (NeuRonNode.this) {
                                log("checkpoint: " + nodes.size() + " nodes");
                                printMembers();
                                printGrid();
                            }
                        } catch (Exception ex) {
                            err(ex);
                        }
                    }
                }, dumpPeriod, dumpPeriod, TimeUnit.SECONDS);
                if (membershipBroadcastPeriod > 0) {
                    scheduler.scheduleAtFixedRate(new Runnable() {
                        public void run() {
                            synchronized (NeuRonNode.this) {
                                try {
                                    if (membersChanged.get()) {
                                        broadcastMembershipChange((short) 0);
                                    }
                                } catch (Exception ex) {
                                    // failure-oblivious: swallow any exceptions and
                                    // just try resuming
                                    err(ex);
                                }
                            }
                        }
                    }, 1, membershipBroadcastPeriod, TimeUnit.SECONDS);
                }
                // do not remove this for now
                Thread.sleep(2000);
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
                                        if (!capJoins || nodes.size() < numNodesHint) {
                                            addMember(nodeId, msg.addr, msg.port, msg.src);
                                            if (nodes.size() == numNodesHint) {
                                                semAllJoined.release();
                                            }
                                            if (blockJoins) {
                                                if (nodes.size() >= numNodesHint) {
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
                                        } else if (capJoins && nodes.size() == numNodesHint) {
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
            try {
                Socket s;
                long startTime = System.currentTimeMillis();
                int count = 0;
                while (true) {
                    if (count++ > joinTimeLimit) {
                        throw new PlannedException("exceeded join try limit; aborting");
                    }
                    // if ((System.currentTimeMillis() - startTime) / 1000 > joinTimeLimit) {
                    //     throw new PlannedException("exceeded join time limit; aborting");
                    // }
                    // Connect to the co-ordinator
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

                // wait for coordinator to announce my existence to others
                Thread.sleep(membershipBroadcastPeriod * 1000);
            } catch (PlannedException ex) {
                throw ex;
            } catch (SocketException ex) {
                log(ex.getMessage());
                return;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            // now start accepting pings and other msgs,
            // also start sending probes and sending out other msgs
            try {
                new DatagramAcceptor().bind(new InetSocketAddress(myCachedAddr, myPort),
                                            new Receiver(), cfg);
                log("server started on " + myCachedAddr + ":" + (basePort + myNid));
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

        ArrayList<Short> sorted_nids = memberNids();
        probeTable[sorted_nids.indexOf(myNid)][sorted_nids.indexOf(nid)] = Short.MAX_VALUE;

        Short nextHop = nextHopTable.get(nid);
        if ((nextHop != null) && (nextHop == myNid)) {
            nextHopTable.remove(nid);
        }
        HashSet<Short> nhSet = nextHopOptions.get(nid);
        if (nhSet != null) {
            for (Iterator<Short> it = nhSet.iterator(); it.hasNext();) {
                if (it.next() == myNid) {
                    it.remove();
                }
            }
        }
    }

    public synchronized void unignore(short nid) {
        log("unignoring " + nid);
        ignored.remove(nid);
    }

    private void pingAll() {
        log("pinging");
        Ping ping = new Ping();
        ping.time = System.currentTimeMillis();
        NodeInfo tmp = nodes.get(myNid);
        ping.info = new NodeInfo();
        ping.info.id = origNid; // note that the ping info uses the original id
        ping.info.addr = tmp.addr;
        ping.info.port = tmp.port;
        for (short nid : nodes.keySet())
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
                        if (nodes.containsKey(msg.src)) {
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
                            if ((!capJoins || nodes.size() < numNodesHint) &&
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
                    } else {
                        // log("recv." + msg.getClass().getSimpleName(), "ignored from " + msg.src + " session " + msg.session);
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
                        //if (ignored.contains(msg.src)) return;
                        log("recv." + msg.getClass().getSimpleName(), "from " + msg.src);

                        // always act on pings/pong (for rtt collection)

                        if (msg instanceof Ping) {
                            Ping ping = ((Ping) msg);
                            Pong pong = new Pong();
                            pong.time = ping.time;
                            sendObject(pong, ping.info);
                        } else if (msg instanceof Pong) {
                            Pong pong = (Pong) msg;
                            short rtt = (short) (System.currentTimeMillis() - pong.time);
                            log("latency", "one way latency to " + pong.src + " = " + rtt/2);
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
                            if (msg instanceof Membership) {
                                Membership m = (Membership) msg;
                                myNid = m.yourId;
                                updateMembers(m.members);
                            } else if (msg instanceof Measurements) {
                                log(((Measurements) msg).toString());
                                updateNetworkState((Measurements) msg);
                            } else if (msg instanceof RoutingRecs) {
                                log(((RoutingRecs) msg).toString());
                                handleRecommendation(((RoutingRecs) msg).recs);
                                log(toStringNextHopTable());
                            } else if (msg instanceof Ping) {
                                // nothing to do, already handled above
                            } else if (msg instanceof Pong) {
                                if (!ignored.contains(msg.src)) {
                                    Pong pong = (Pong) msg;
                                    resetTimeoutAtNode(pong.src);
                                    short rtt = (short) (System.currentTimeMillis() - pong.time);
                                    ArrayList<Short> sortedNids = memberNids();
                                    int i = sortedNids.indexOf(myNid), j = sortedNids.indexOf(pong.src);
                                    probeTable[i][j] = (short) (
                                            smoothingFactor * (rtt / 2) +
                                            (1 - smoothingFactor) * probeTable[i][j]);
                                }
                            } else if (msg instanceof PeeringRequest) {
                                PeeringRequest pr = (PeeringRequest) msg;
                                GridNode newNeighbor = new GridNode();
                                newNeighbor.id = pr.src;
                                newNeighbor.isAlive = true;
                                overflowNeighbors.add(newNeighbor);
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
    private int timeout;
    private Hashtable<Short, ScheduledFuture<?>> timeouts = new Hashtable<Short, ScheduledFuture<?>>();

    /**
     * a coord-only method
     *
     * @param nid
     */
    private void resetTimeoutAtCoord(final short nid) {
        if (nodes.containsKey(nid)) {
            ScheduledFuture<?> oldFuture = timeouts.get(nid);
            if (oldFuture != null) {
                oldFuture.cancel(false);
            }
            ScheduledFuture<?> future = scheduler.schedule(new Runnable() {
                public void run() {
                    try {
                        synchronized (NeuRonNode.this) {
                            removeMember(nid);
                        }
                    } catch (Exception ex) {
                        err(ex);
                    }
                }
            }, timeout, TimeUnit.SECONDS);
            timeouts.put(nid, future);
        }
    }

    private void resetTimeoutAtNode(final short nid) {
        if (nodes.containsKey(nid)) {
            ScheduledFuture<?> oldFuture = timeouts.get(nid);
            if (oldFuture != null) {
                oldFuture.cancel(false);
            }
            for (short i = 0; i < numRows; i++) {
                for (short j = 0; j < numCols; j++) {
                    if (grid[i][j].id == nid) {
                        grid[i][j].isAlive = true;
                    }
                }
            }
            ScheduledFuture<?> future = scheduler.schedule(new Runnable() {
                public void run() {
                    try {
                        synchronized (NeuRonNode.this) {
                            // O(n)
                            for (short i = 0; i < numRows; i++) {
                                for (short j = 0; j < numCols; j++) {
                                    if (grid[i][j].id == nid) {
                                        grid[i][j].isAlive = false;
                                    }
                                }
                            }
                            ArrayList<Short> sorted_nids = memberNids();
                            probeTable[sorted_nids.indexOf(myNid)][sorted_nids.indexOf(nid)] = Short.MAX_VALUE;
                        }
                    } catch (Exception ex) {
                        err(ex);
                    }
                }
            }, failoverTimeout, TimeUnit.SECONDS);
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
        nodes.put(newNid, info);
        id2id.put(newNid, origId);
        id2name.put(newNid, addr.getHostName());
        addr2id.put(addr, newNid);
        log("adding new node: " + newNid + " oid " + origId + " name " +
                id2name.get(newNid));
        currentStateVersion++;
        resetTimeoutAtCoord(newNid);
        return info;
    }

    private ArrayList<Short> memberNids() {
        if ((cachedMemberNidsVersion < currentStateVersion) || (cachedMemberNids == null) ) {
            //log("NEW cachedMemberNids (" + cachedMemberNidsVersion + ", " + currentStateVersion);
            cachedMemberNidsVersion = currentStateVersion;
            cachedMemberNids = new ArrayList<Short>(nodes.keySet());
            Collections.sort(cachedMemberNids);
            //log("Size = " + cachedMemberNids.size());
        }
        return cachedMemberNids;
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
        return new ArrayList<NodeInfo>(nodes.values());
    }

    /**
     * a coordinator-only method
     *
     * throttles these messages so they're sent at most once per second
     */
    private void sendMembership(short nid) {
        Membership msg = new Membership();
        msg.yourId = nid;
        //Long last = lastSentMbr.get(nid);
        //if (last == null || System.currentTimeMillis() - last.longValue() > 1000) {
        //    scheduler.schedule();
        //} else {
        //    scheduler.schedule();
        //}
        //lastSentMbr.put(msg.src, msg.id);
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
        NodeInfo info = nodes.remove(nid);
        Short mid = addr2id.remove(info.addr);
        assert mid != null;
        currentStateVersion++;
        broadcastMembershipChange(nid);
    }

    private void updateMembers(List<NodeInfo> newNodes) {
        List<Short> oldNids = getUncachedmemberNids();
        nodes.clear();

        for (NodeInfo node : newNodes) {
            nodes.put(node.id, node);
        }

        Hashtable<Short, Short> newNextHopTable = new Hashtable<Short, Short>(nodes.size());
        Hashtable<Short, HashSet<Short>> newNextHopOptions = new Hashtable<Short, HashSet<Short>>(nodes.size());

        for (NodeInfo node : newNodes) {
            if (node.id != myNid) {
                Short nextHop = nextHopTable.get(node.id);
                if (nextHop == null) {
                    // new node !
                    /*
                    newNextHopTable.put(node.id, myNid);
                    HashSet<Short> nextHops = new HashSet<Short>();
                    nextHops.add(myNid);
                    newNextHopOptions.put(node.id, nextHops);
                    */
                }
                else {
                    // check if this old next hop is in the new membership list
                    if (nodes.get(nextHop) != null) {
                        // we have some next hop that is alive - leave it as is
                        newNextHopTable.put(node.id, nextHop);
                    }
                    else {
                        // the next hop vanaished. i am next hop to this node now
                        /*
                        newNextHopTable.put(node.id, myNid);
                        */
                    }
                    // of all the possible next hop options to the node,
                    // remove those that are dead.
                    HashSet<Short> nextHops = nextHopOptions.get(node.id);
                    if (nextHops != null) {
                       for (Iterator<Short> it = nextHops.iterator (); it.hasNext (); ) {
                            Short someNextHop = it.next();
                            if (nodes.get(someNextHop) == null) {
                                it.remove ();
                            }
                       }
                       newNextHopOptions.put(node.id, nextHops);
                    } else {
                        /*
                        HashSet<Short> nh = new HashSet<Short>();
                        nextHops.add(myNid);
                        newNextHopOptions.put(node.id, nh);
                        */
                    }
                }
            }
            else {
                //newNextHopTable.put(myNid, myNid);
            }
        }
        nextHopTable = newNextHopTable; // forget about the old one
        nextHopOptions = newNextHopOptions;

        repopulateGrid();
        repopulateProbeTable(oldNids);
        // printGrid();
        log("new state version: " + currentStateVersion);
        log(toStringNeighborList());
    }

    private void repopulateGrid() {
        numCols = (short) Math.ceil(Math.sqrt(nodes.size()));
        numRows = (short) Math.ceil((double) nodes.size() / (double) numCols);
        grid = new GridNode[numRows][numCols];
        List<Short> nids = memberNids();
        short m = 0;
        for (short i = 0; i < numRows; i++) {
            for (short j = 0; j < numCols; j++) {
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

    public static enum RoutingScheme { SIMPLE, SQRT, SQRT_NOFAILOVER, SQRT_RC_FAILOVER, SQRT_SPECIAL };
    private final RoutingScheme scheme;

    private HashSet<GridNode> getNeighborList() {
        HashSet<GridNode> neighborSet = new HashSet<GridNode>();
        // iterate over all grid positions, looking for self
        for (short r = 0; r < numRows; r++) {
            for (short c = 0; c < numCols; c++) {

                // this can happen at most twice
                if (scheme == RoutingScheme.SIMPLE) {
                    neighborSet.add(grid[r][c]);
                } else if (grid[r][c].id == myNid) {
                    // all the nodes in row i, and all the nodes in column j are
                    // belong to us :)

                    // O(N^1.5)   :(
                    // for each node in this column that's not me
                    for (short x = 0; x < numCols; x++) {
                        if (grid[r][x].id != myNid) {
                            GridNode neighbor = grid[r][x];
                            // if they're alive, then add them a neighbor and move on
                            if (neighbor.isAlive && !ignored.contains(neighbor.id)) {
                                neighborSet.add(neighbor);
                            } else if (scheme != RoutingScheme.SQRT_NOFAILOVER) {
                                // for each node in this row that's not me
                                for (short i = 0; i < numRows; i++) {
                                    if ( (i != r) && ((grid[i][c].isAlive == false) ||  ignored.contains(grid[i][c].id)) ) {
                                        /* (r, x) and (i, c) can't be reached
                                         * (i, x) needs a failover R node
                                         */
                                        log("R node failover!");
                                        boolean bFoundReplacement = false;
                                        // within that failure column, search for a failover
                                        for (short j = 0; j < numCols; j++) {
                                            if ( (grid[i][j].id != myNid) && (grid[i][j].isAlive == true) && !ignored.contains(grid[i][j].id)) {
                                                // request them as a failover and add them as a neighbor
                                                PeeringRequest pr = new PeeringRequest();
                                                sendObject(pr, grid[i][j].id);
                                                neighborSet.add(grid[i][j]);
                                                log("Failing over (Row) to node " + grid[i][j] + " as R node for node " + grid[i][x]);
                                                bFoundReplacement = true;
                                                break;
                                            }
                                        }
                                        // if no failover found
                                        if ((bFoundReplacement == false) && ((scheme == RoutingScheme.SQRT_RC_FAILOVER) || (scheme == RoutingScheme.SQRT_SPECIAL))) {
                                            // within that failure row, search for a failover
                                            for (short j = 0; j < numRows; j++) {
                                                if ( (grid[j][x].id != myNid) && (grid[j][x].isAlive == true) && !ignored.contains(grid[j][x].id)) {
                                                    // request them as a failover and add them as a neighbor
                                                    PeeringRequest pr = new PeeringRequest();
                                                    sendObject(pr, grid[j][x].id);
                                                    neighborSet.add(grid[j][x]);
                                                    log("Failing over (Column) to node " + grid[j][x] + " as R node for node " + grid[i][x]);
                                                    bFoundReplacement = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for (short x = 0; x < numRows; x++) {
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
        for (short r = 0; r < numRows; r++) {
            for (short c = 0; c < numCols; c++) {
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
    private void repopulateProbeTable(List<Short> oldNids) {
        short newProbeTable[][] = new short[nodes.size()][nodes.size()];

        int nodeIndex = memberNids().indexOf(myNid);
        for (int i = 0; i < memberNids().size(); i++) {
            if (i == nodeIndex) {
                newProbeTable[i][i] = 0;
            } else {
                newProbeTable[nodeIndex][i] = Short.MAX_VALUE;
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
        for (Short memberId : memberNids()) {
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
        for (Short node : nextHopTable.keySet()) {
            s += node + " -> " + nextHopTable.get(node) + "; ";
        }
        s += "]";
        return s;
    }

    private void printMembers() {
        String s = "members:";
        for (NodeInfo node : nodes.values()) {
            s += "\n  " + node.id + " oid " + id2id.get(node.id) + " " +
                id2name.get(node.id) + " " + node.port;
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

    private void printProbeTable() {
        ArrayList<Short> sorted_nids = memberNids();
        int myIndex = sorted_nids.indexOf(myNid);

        String s = new String("Adj table for " + myNid
                + " = [");
        for (int i = 0; i < probeTable[myIndex].length; i++) {
            s += sorted_nids.get(i) + ":" + probeTable[myIndex][i] + "; ";
        }
        s += "]";
        log(s);
    }

    private void printProbeTable(int probeTableOffset) {
        ArrayList<Short> sorted_nids = memberNids();

        String s = new String("Adj table for " + sorted_nids.get(probeTableOffset)
                + " = [");
        for (int i = 0; i < probeTable[probeTableOffset].length; i++) {
            s += sorted_nids.get(i) + ":" + probeTable[probeTableOffset][i] + "; ";
        }
        s += "]";
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
        ArrayList<Short> sortedNids = memberNids();
        int totalSize = 0;
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
                    rec.via = sortedNids.get(mini);
                    recs.add(rec);

                    /*
                    /// DEBUG
                    if ( (myNid == 2) && (src.id == 1) && (dst.id == 3) ) {
                        System.out.println("Reco : " + src.id + "->" + rec.via + "->" + dst.id);
                    }
                    */
                }
            }
            RoutingRecs msg = new RoutingRecs();
            msg.recs = recs;
            totalSize += sendObject(msg, src.id);
        }
        log("Sending recommendations to neighbors, total " + totalSize + " bytes. " + toStringNeighborList());
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
        ArrayList<Short> sortedNids = memberNids();

        HashSet<GridNode> others = getOtherMembers();
        others.removeAll(nl);

        int totalSize = 0;
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
                            mini = i;
                        }
                    }
                    Rec rec = new Rec();
                    rec.dst = dst.id;
                    rec.via = sortedNids.get(mini);
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
                    rec.via = (short)mini;
                    recs.add(rec);
                }
            }

            RoutingRecs msg = new RoutingRecs();
            msg.recs = recs;
            totalSize += sendObject(msg, src.id);
        }
        log("Sending recommendations to neighbors, total " + totalSize + " bytes. " + toStringNeighborList());
    }

//    /**
//     * caches names
//     */
//    private String id2nm(short nid, String addr) {
//        if (nid >= 0) {
//            String name = id2name.get(nid);
//            if (name == null) {
//                NodeInfo node = nodes.get(nid);
//                if (node != null) {
//                    name = node.getHostName();
//                    id2name.put(nid, name);
//                } else {
//                    id2name.put(nid, node);
//                }
//            }
//            return name;
//        } else {
//            return addr;
//        }
//    }

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
            sendSocket.send(new DatagramPacket(buf, buf.length, addr, port));
            return buf.length;
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

    private int sendObject(final Msg o, NodeInfo info, short nid) {
        return sendObject(o, info.addr, info.port, nid);
    }

    private int sendObject(final Msg o, NodeInfo info) {
        return sendObject(o, info, (short)-1);
    }

    private int sendObject(final Msg o, short nid) {
        return nid != myNid && !ignored.contains(nid) ?
            sendObject(o, nid == 0 ? coordNode : nodes.get(nid), nid) : 0;
    }

    private void broadcastMeasurements() {
        Measurements rm = new Measurements();
        //rm.membershipList = memberNids();
        ArrayList<Short> sorted_nids = memberNids();
        rm.probeTable = probeTable[sorted_nids.indexOf(myNid)].clone();
        HashSet<GridNode> nl = getNeighborList();
        int totalSize = 0;
        for (GridNode neighbor : nl) {
            totalSize += sendObject(rm, neighbor.id);
        }
        log("Sending measurements to neighbors, total " + totalSize + " bytes. " + toStringNeighborList());
        //printProbeTable();
    }

    private void updateNetworkState(Measurements m) {
        int offset = memberNids().indexOf(m.src);
        // Make sure that we have the exact same world-views before proceeding,
        // as otherwise the neighbor sets may be completely different. Steps can
        // be taken to tolerate differences and to give best-recommendations
        // based on incomplete info, but it may be better to take a step back
        // and re-evaluate our approach to consistency as a whole first. For
        // now, this simple central-coordinator approach will at least work.
        //if (offset != -1 && m.membershipList.equals(memberNids())) {

        if (offset != -1) {
            for (int i = 0; i < m.probeTable.length; i++) {
                probeTable[offset][i] = m.probeTable[i];
            }
        }
        //printProbeTable(offset);
    }

    private void handleRecommendation(ArrayList<Rec> recs) {
        if (recs != null) {
            for (Rec r : recs) {
                // For the algorithm where the R-points only send recos about their neighbors:
                // For each dst - only 2 nodes can tell us about the best hop to dst.
                // They are out R-points. Trust them and update your entry blindly.
                // For the algorithm where the R-points only send recos about
                // everyone else this logic will have to be more complex
                // (like check if the reco was better)

                if ( (!ignored.contains(r.via)) || ((r.via == myNid) && !ignored.contains(r.dst)) ) {
                    nextHopTable.put(r.dst, r.via);


                    /*
                    /// DEBUG
                    if (myNid == 1) {
                        System.out.println("GOOOOOOOOOOOO " + r.dst + " VIAAAAA " + r.via);
                        log("GOOOOOOOOOOOO " + r.dst + " VIAAAAA " + r.via);
                    }
                    */

                    HashSet<Short> nextHops = nextHopOptions.get(r.dst);
                    if (nextHops == null) {
                        nextHops = new HashSet<Short>();
                        nextHopOptions.put(r.dst, nextHops);
                    }
                    nextHops.add(r.via);
                }
            }
        }

        // remove later !!!
        log(toStringNextHopTable());
        int reachable = 0;
        int oneHopPathsAvailable = 0;
        for (short nid : nextHopTable.keySet()) {
            int nextHop = nextHopTable.get(nid);
            if (nextHop != myNid) {
                if (!ignored.contains(nextHop)) {
                    reachable++;
                } else {
                    //log("@@@ not considering " + nextHop);
                }
            } else if (!ignored.contains(nid)) {
                reachable++;
            } else {
                //log("$$$ not considering " + nextHop);
            }

            HashSet<Short> nhSet = nextHopOptions.get(nid);
            if (nhSet != null) {
                for (Iterator<Short> it = nhSet.iterator(); it.hasNext();) {
                    Short someNextHop = it.next();
                    if (!ignored.contains(someNextHop)) {
                        oneHopPathsAvailable++;
                    }
                }
                if (!nhSet.contains(myNid) && !ignored.contains(nid)) {
                    oneHopPathsAvailable++;
                }
            }
        }

        // this crap is needed because if eveyone but one member (X) in your row has died
        // then there is no one to tell you about X
        Set<Short> knownMembers = new HashSet<Short>(nodes.keySet());
        Set<Short> recoBasedReachableMembers = nextHopTable.keySet();

        knownMembers.removeAll(recoBasedReachableMembers);
        for (short nid : knownMembers) {
            if (!ignored.contains(nid) && (nid != myNid)) {
                reachable++;
                oneHopPathsAvailable++;
            }
        }


        int avgOneHopsAvailable = oneHopPathsAvailable / reachable;
        log("Reachability Count = " + reachable + " of " + (nodes.size() - 1)
                + " nodes in 1 or less hops.");
        log("Avg # of one hop or direct paths available = "
                + avgOneHopsAvailable);
    }

    public void quit() {
        this.doQuit.set(true);
    }

}

class GridNode {
    public short id;
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
short id;
int port;
InetAddress addr;
}
class Rec  {
short dst;
short via;
}
class Msg  {
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
}
class MemberPoll extends Msg {
}
class PeeringRequest extends Msg {
}

      class Serialization {
    

      public void serialize(Object obj, DataOutputStream out) throws IOException {
      if (false) {}
      
else if (obj.getClass() == NodeInfo.class) {
NodeInfo casted = (NodeInfo) obj; out.writeInt(0);
out.writeShort(casted.id);
out.writeInt(casted.port);
byte[] buf = casted.addr.getAddress();out.writeInt(buf.length);out.write(buf);
}
else if (obj.getClass() == Rec.class) {
Rec casted = (Rec) obj; out.writeInt(1);
out.writeShort(casted.dst);
out.writeShort(casted.via);
}
else if (obj.getClass() == Msg.class) {
Msg casted = (Msg) obj; out.writeInt(2);
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == Join.class) {
Join casted = (Join) obj; out.writeInt(3);
byte[] buf = casted.addr.getAddress();out.writeInt(buf.length);out.write(buf);
out.writeInt(casted.port);
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == Init.class) {
Init casted = (Init) obj; out.writeInt(4);
out.writeShort(casted.id);
 out.writeInt(casted.members.size()); 
for (int i = 0; i < casted.members.size(); i++) {
out.writeShort(casted.members.get(i).id);
out.writeInt(casted.members.get(i).port);
byte[] buf = casted.members.get(i).addr.getAddress();out.writeInt(buf.length);out.write(buf);
}
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == Membership.class) {
Membership casted = (Membership) obj; out.writeInt(5);
 out.writeInt(casted.members.size()); 
for (int i = 0; i < casted.members.size(); i++) {
out.writeShort(casted.members.get(i).id);
out.writeInt(casted.members.get(i).port);
byte[] buf = casted.members.get(i).addr.getAddress();out.writeInt(buf.length);out.write(buf);
}
out.writeShort(casted.numNodes);
out.writeShort(casted.yourId);
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == RoutingRecs.class) {
RoutingRecs casted = (RoutingRecs) obj; out.writeInt(6);
 out.writeInt(casted.recs.size()); 
for (int i = 0; i < casted.recs.size(); i++) {
out.writeShort(casted.recs.get(i).dst);
out.writeShort(casted.recs.get(i).via);
}
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == Ping.class) {
Ping casted = (Ping) obj; out.writeInt(7);
out.writeLong(casted.time);
out.writeShort(casted.info.id);
out.writeInt(casted.info.port);
byte[] buf = casted.info.addr.getAddress();out.writeInt(buf.length);out.write(buf);
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == Pong.class) {
Pong casted = (Pong) obj; out.writeInt(8);
out.writeLong(casted.time);
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == Measurements.class) {
Measurements casted = (Measurements) obj; out.writeInt(9);
out.writeInt(casted.probeTable.length);
for (int i = 0; i < casted.probeTable.length; i++) {
out.writeShort(casted.probeTable[i]);
}
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == MemberPoll.class) {
MemberPoll casted = (MemberPoll) obj; out.writeInt(10);
out.writeShort(casted.src);
out.writeShort(casted.version);
out.writeShort(casted.session);
}
else if (obj.getClass() == PeeringRequest.class) {
PeeringRequest casted = (PeeringRequest) obj; out.writeInt(11);
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
return obj;}
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
return obj;}
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
return obj;}
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
return obj;}
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
return obj;}
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
return obj;}
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
return obj;}
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
return obj;}
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
return obj;}
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
return obj;}

    default:throw new RuntimeException("unknown obj type");}}

    private byte[] readBuffer = new byte[4];

    public int readInt(DataInputStream dis) throws IOException {
      dis.readFully(readBuffer, 0, 4);
      return (
        ((int)(readBuffer[0] & 255) << 24) +
        ((readBuffer[1] & 255) << 16) +
        ((readBuffer[2] & 255) <<  8) +
        ((readBuffer[3] & 255) <<  0));
    }

    /*
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
      m.ProbeTable = new long[5];
      m.ProbeTable[1] = 7;
      m.ProbeTable[2] = 8;
      m.ProbeTable[3] = 9;

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
    }*/
    }
