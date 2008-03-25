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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.util.logging.Formatter;
import java.nio.ByteBuffer;

//import org.apache.mina.common.ByteBuffer;
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
    private final Reactor scheduler;
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

    // These variables used internally to updateMembers (keep this way, for
    // abstraction purposes), and are only exposed for printGrid().
    private NodeState[][] grid;
    private short numCols, numRows;

    // Coord-only: maps addresses to nids
    private final Hashtable<InetAddress, Short> addr2id = new Hashtable<InetAddress, Short>();

    // Specific to this node. Lookup from destination to default rs's to it
    private final Hashtable<Short, HashSet<NodeState>> defaultRendezvousServers =
        new Hashtable<Short, HashSet<NodeState>>();

    // Lookup from node to a set of its rendezvous servers
    private final Hashtable<NodeState, HashSet<NodeState>> nodeDefaultRSs =
        new Hashtable<NodeState, HashSet<NodeState>>();

    private short currentStateVersion;

    public final int neighborBroadcastPeriod;
    public final int probePeriod;
    public final int gcPeriod;

    private final NodeInfo coordNode;

    private final RunMode mode;
    private final short numNodesHint;
    private final Runnable semAllJoined;

    private final Random rand = new Random();

    private final InetAddress myCachedAddr;
    private ArrayList<Short> cachedMemberNids = new ArrayList<Short>(); // sorted list of members
    private short cachedMemberNidsVersion;
    private final boolean blockJoins;
    private final boolean capJoins;
    private final int joinRetries; // seconds

    private final boolean enableSubpings;
    private final int subpingPeriod; // seconds

    // number of intervals which we'll split the probing time into
    private int numProbeIntervals;

    private Set[] pingTable;
    private final Hashtable<NodeState, Integer> pingId = new Hashtable<NodeState, Integer>();

    private final int dumpPeriod;

    private final FileHandler fh;
    private final short origNid;

    private final short sessionId;
    private final int linkTimeout;

    private final int membershipBroadcastPeriod;

    private static final String defaultLabelSet = "subprobe send.Ping recv.Ping stale.Ping send.Pong recv.Pong send.Subprobe recv.Subprobe stale.Pong send.Measurements send.RoutingRecs subprobe";

    private final Hashtable<Short,Long> lastSentMbr = new Hashtable<Short,Long>();

    private final double smoothingFactor;
    private final short resetLatency = Short.MAX_VALUE;

    private final DatagramAcceptor acceptor;

    private final Hashtable<Short, NodeInfo> coordNodes = new Hashtable<Short, NodeInfo>();

    private final ArrayList<Short> memberNids = new ArrayList<Short>();

    private final ArrayList<NodeState> otherNodes = new ArrayList<NodeState>();

    private final ArrayList<NodeState> lastRendezvousServers = new ArrayList<NodeState>();

    private final double directBonus, hysteresisBonus;

    private final long startTime = System.currentTimeMillis();

    private final int pingDumpPeriod, pingDumpInitialDelay;

    private final Reactor reactor;

    private Runnable safeRun(final Runnable r) {
        return new Runnable() {
            public void run() {
                try {
                    r.run();
                } catch (Throwable ex) {
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

    private final int joinDelay;

    public NeuRonNode(short id,
                        Properties props, short numNodes, Runnable semJoined,
                        InetAddress myAddr, String coordinatorHost, NodeInfo coordNode,
                        DatagramAcceptor acceptor, Reactor reactor) {

        this.reactor = reactor;

        joinDelay = rand.nextInt(Integer.parseInt(props.getProperty("joinDelayRange", "1")));

        if ((coordNode == null) || (coordNode.addr == null)){
            throw new RuntimeException("coordNode is null!");
        }

        dumpPeriod = Integer.parseInt(props.getProperty("dumpPeriod", "60"));

        myNid = id;
        origNid = id;
        cachedMemberNidsVersion = (short)-1;
        joinRetries = Integer.parseInt(props.getProperty("joinRetries", "10")); // wait up to 10 secs by default for coord to be available
        membershipBroadcastPeriod = Integer.parseInt(props.getProperty("membershipBroadcastPeriod", "0"));

        // NOTE note that you'll probably want to set this, always!
        sessionId = Short.parseShort(props.getProperty("sessionId", "0"));

        blockJoins = Boolean.valueOf(props.getProperty("blockJoins", "true"));
        capJoins = Boolean.valueOf(props.getProperty("capJoins", "true"));

        this.coordinatorHost = coordinatorHost;
        this.coordNode = coordNode;

        mode = RunMode.valueOf(props.getProperty("mode", "sim").toUpperCase());
        basePort = coordNode.port;
        scheme = RoutingScheme.valueOf(props.getProperty("scheme", "SQRT").toUpperCase());
        if (scheme == RoutingScheme.SQRT) {
        	neighborBroadcastPeriod = Integer.parseInt(props.getProperty("neighborBroadcastPeriod", "15"));
        } else {
        	neighborBroadcastPeriod = Integer.parseInt(props.getProperty("neighborBroadcastPeriod", "30"));
        }
        gcPeriod = Integer.parseInt(props.getProperty("gcPeriod", neighborBroadcastPeriod + ""));
        enableSubpings = Boolean.valueOf(props.getProperty("enableSubpings", "true"));

        this.acceptor = acceptor;

        // for simulations we can safely reduce the probing frequency, or even turn it off
        //if (mode == RunMode.SIM) {
            //probePeriod = Integer.parseInt(props.getProperty("probePeriod", "60"));
        //} else {
            probePeriod = Integer.parseInt(props.getProperty("probePeriod", "30"));
        //}
        subpingPeriod = Integer.parseInt(props.getProperty("subpingPeriod", "" + probePeriod));
        membershipTimeout = Integer.parseInt(props.getProperty("timeout", "" + 30*60));
        linkTimeout = Integer.parseInt(props.getProperty("failoverTimeout", "" + membershipTimeout));

        pingDumpInitialDelay = Integer.parseInt(props.getProperty("pingDumpInitialDelay", "60"));
        pingDumpPeriod = Integer.parseInt(props.getProperty("pingDumpPeriod", "60"));

        // Events are when simulated latencies change; these are substituted in
        // for real measured latencies, and can be useful in simulation. These
        // events must be specified in time order! To remove any sim latency
        // for a dst, set it to resetLatency.
        String simEventsSpec = props.getProperty("simEvents", "");
        if (!simEventsSpec.equals("")) {
            String[] events = simEventsSpec.split(";");
            for (String e : events) {
                String[] parts = e.split(" ");
                int secs = Integer.parseInt(parts[0]);
                short oid = Short.parseShort(parts[1]);
                if (oid == myNid) {
                    short dst = Short.parseShort(parts[2]);
                    short lat = Short.parseShort(parts[3]);
                    simEvents.addLast(new SimEvent(secs, oid, dst, lat));
                }
            }
        }

        smoothingFactor = Double.parseDouble(props.getProperty("smoothingFactor", "0.1"));
        directBonus = Double.parseDouble(props.getProperty("directBonus", "1.05"));
        hysteresisBonus = Double.parseDouble(props.getProperty("hysteresisBonus", "1.05"));

        Formatter minfmt = new Formatter() {
            public String format(LogRecord record) {
                StringBuilder buf = new StringBuilder();
                buf.append(record.getMillis()).append(' ')/*.append(new Date(record.getMillis())).append(" ").append(
                        record.getLevel()).append(" ")*/.append(
                        record.getLoggerName()).append(": ").append(
                        record.getMessage()).append("\n");
                return buf.toString();
            }
        };
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
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        this.scheduler = reactor;
        grid = null;
        numCols = numRows = 0;
        isCoordinator = myNid == 0;
        currentStateVersion = (short) (isCoordinator ? 0 : -1);

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
        this.myAddr = new InetSocketAddress(myCachedAddr, myPort);

        clientTimeout = Integer.parseInt(props.getProperty("clientTimeout", "" + 3 * neighborBroadcastPeriod));
    }

    private final int myPort;
    private final InetSocketAddress myAddr;

    private void handleInit(Init im) {
        if (im.id == -1) {
            throw new PlannedException("network is full; aborting");
        }
        myNid = im.id;
        logger = Logger.getLogger("node_" + myNid);
        if (logger.getHandlers().length == 0) {
            logger.addHandler(fh);
        }
        currentStateVersion = im.version;
        log("got from coord => Init version " + im.version);
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

    public void err(Throwable ex) {
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

    public static final class SimEvent {
        public int secs;
        public short oid, dst, lat;
        public SimEvent(int secs, short src, short dst, short lat) {
            this.secs = secs;
            this.oid = src; // TODO check this, originally a bug: this.oid = oid;
            this.dst = dst;
            this.lat = lat;
        }
    }

    public final ArrayDeque<SimEvent> simEvents = new ArrayDeque<SimEvent>();
    public final ShortShortMap simLatencies = new ShortShortMap(resetLatency);

    public static final class PlannedException extends RuntimeException {
        public PlannedException(String msg) {
            super(msg);
        }
    }

    public final AtomicReference<Exception> failure = new AtomicReference<Exception>();
    public void run() {
        try {
            run3();
        } catch (PlannedException ex) {
            warn(ex.getMessage());
            failure.set(ex);
            if (semAllJoined != null) semAllJoined.run();
        } catch (Exception ex) {
            err(ex);
            failure.set(ex);
            if (semAllJoined != null) semAllJoined.run();
        }
    }

    /**
     * Similar to fixed-rate scheduling, but doesn't try to make up multiple
     * overdue items, but rather allows us to skip over them. This should deal
     * better with PLab's overloaded hosts.
     *
     * @param r The runnable task.
     * @param initialDelay The initial delay in seconds.
     * @param period The period in seconds.
     */
    private ScheduledFuture<?> safeSchedule(final Runnable r, long initialDelay, final long period) {
        final long bufferTime = 1000; // TODO parameterize
        return scheduler.schedule(new Runnable() {
            private long scheduledTime = -1;
            public void run() {
                if (scheduledTime < 0) scheduledTime = System.currentTimeMillis();
                r.run();
                long now = System.currentTimeMillis();
                scheduledTime = Math.max(scheduledTime + period * 1000, now + bufferTime);
                scheduler.schedule(this, scheduledTime - now, TimeUnit.MILLISECONDS);
            }
        }, initialDelay, TimeUnit.SECONDS);
    }

    private boolean hasJoined = false;
    private Session session = null;

    public void run3() {
        if (isCoordinator) {
            try {
                safeSchedule(safeRun(new Runnable() {
                    public void run() {
                        log("checkpoint: " + coordNodes.size() + " nodes");
                        printMembers();
                        //printGrid();
                    }
                }), dumpPeriod, dumpPeriod);
                if (false) {
                    acceptor.bind(new InetSocketAddress(InetAddress
                            .getLocalHost(), basePort), new CoordReceiver());
                } else {
                    final CoordHandler handler = new CoordHandler();
                    session = reactor.register(null, myAddr, handler);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            try {
                if (false) {
                    final Receiver receiver = new Receiver();
                    acceptor.bind(new InetSocketAddress(myCachedAddr, myPort),
                                                receiver);
                } else {
                    final NodeHandler handler = new NodeHandler();
                    session = reactor.register(null, myAddr, handler);
                }

                log("server started on " + myCachedAddr + ":" + (basePort + myNid));

                // Ping the coordinator (special case)
                scheduler.scheduleWithFixedDelay(safeRun(new Runnable() {
                    public void run() {
                        if (hasJoined) {
			    pingCoord();
                        }
                    }
                }), 7, probePeriod, TimeUnit.SECONDS);


                safeSchedule(safeRun(new Runnable() {
                    public void run() {
                        if (hasJoined) {
                            /*
                             * path-finding and rendezvous finding is
                             * interdependent. the fact that we do the path-finding
                             * first before the rendezvous servers is arbitrary.
                             */
			    // TODO the below can be decoupled. Actually, with the current bug, node.hop isn't
			    //    set and findPathsForAllNodes() doesn't actually do anything.
                            Pair<Integer, Integer> p = findPathsForAllNodes();
                            log(p.first
                                    + " live nodes, "
                                    + p.second
                                    + " avg paths, "
                                    + nodes.get(myNid).latencies.keySet()
                                            .size() + " direct paths");
			    // TODO this can also be decoupled. However, we don't want too much time to pass
			    // between calculating rendezvous servers and actually sending to them, since in
			    // the mean time we will have received recommendations which hint at remote failures
			    // etc. Also our notion of isReachable will have changed. For SIMPLE this is easy:
			    // we can remove from the set any nodes that are no longer reachable. An ad-hoc
			    // solution would be to run it just once and then check if the dst is reachable before
			    // broadcasting. This might take longer in certain failure scenarios.
			    // We can, before sending, check whether link is down or we received a more recent
			    // measurement packing showing remote failure. If remote failure, we'd like to wait
			    // anyway since dst might be down. Might be useless, but can still send measurements.
			    // If link is down, just don't send anything. We'll try again in the next iteration.
			    // SUMMARY: after constructing measRecips, put each destination onto the queue,
			    //       and if when popped !dst.isReachable, just don't send.
                            ArrayList<NodeState> measRecips = scheme == RoutingScheme.SIMPLE ? getAllReachableNodes()
                                    : getAllRendezvousServers();
			    // TODO this can also be decoupled, and also split up
			    //   into intervals. We should keep the probeTable[]
			    //   in memory and always up to date. Further optimization
			    //   is to keep array of bytes, so no serialization.
                            broadcastMeasurements(measRecips);
			    // TODO this can also be decoupled. Don't use
			    //   getAllRendezvousClients(), just work directly with
			    //   the list. Order doesn't matter (confirm).
			    //   Also split calls to findHops into intervals.
                            if (scheme != RoutingScheme.SIMPLE) {
                                broadcastRecommendations();
                            }
                        }
                    }
                }), 7, neighborBroadcastPeriod);

                scheduler.scheduleWithFixedDelay(safeRun(new Runnable() {
                    public void run() {
                        log("received/sent " + pingpongCount + " pings/pongs "
                                + pingpongBytes + " bytes");
                        log("received/sent " + subprobeCount + " subprobes "
                                + subprobeBytes + " bytes");
                    }
                }), pingDumpInitialDelay, pingDumpPeriod, TimeUnit.SECONDS);

                final InetAddress coordAddr = InetAddress.getByName(coordinatorHost);
                scheduler.schedule(safeRun(new Runnable() {
                    private int count = 0;
                    public void run() {
                        if (count > joinRetries) {
                            warn("exceeded max tries!");
                            System.exit(0);
                        } else if (!hasJoined) {
                            log("sending join to coordinator at "
                                    + coordinatorHost + ":" + basePort
                                    + " (try " + count++ + ")");
                            Join msg = new Join();
                            msg.addr = myCachedAddr;
                            msg.src = myNid; // informs coord of orig id
                            msg.port = myPort;
                            sendObject(msg, coordAddr, basePort, (short)-1);
                            log("waiting for InitMsg");
                            scheduler.schedule(this, 10, TimeUnit.SECONDS);
                        }
                    }
                }), joinDelay, TimeUnit.SECONDS);
                if (semAllJoined != null) semAllJoined.run();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private final HashSet<Short> ignored = new HashSet<Short>();

    public synchronized void ignore(short nid) {
        if (nid != myNid) {
            log("ignoring " + nid);
            ignored.add(nid);
        }
    }

    public synchronized void unignore(short nid) {
        if (nid != myNid) {
            log("unignoring " + nid);
            ignored.remove(nid);
        }
    }

    private ArrayList<NodeState> getAllReachableNodes() {
        ArrayList<NodeState> nbrs = new ArrayList<NodeState>();
        for (NodeState n : otherNodes)
            if (n.isReachable)
                nbrs.add(n);
        return nbrs;
    }

    private static final byte SUBPING = 0, SUBPING_FWD = 1, SUBPONG = 2,
            SUBPONG_FWD = 3;

    private Subprobe subprobe(InetSocketAddress nod, long time, byte type) {
        Subprobe p = new Subprobe();
        p.src = myAddr;
        p.nod = nod;
        p.time = time;
        p.type = type;
        return p;
    }

//    private int subping(int pingIter) {
//        // We will only subping a fraction of the nodes at this iteration
//        // Note: this synch. statement is redundant until we remove global lock
//        List<Short> nids = new ArrayList<Short>();
//        int initCount = subprobeCount;
//        for (Object obj : pingTable[pingIter]) {
//            NodeState dst = (NodeState) obj;
//            // TODO: dst.hop almost always != 0 (except when dst is new node)
//            subpingPeer(dst);
//        }
//
//        if (subprobeBytes > initBytes) {
//            log("sent subpings " + bytes + " bytes, to " + nids);
//        }
//
//        return subprobeCount - initCount;
//    }

    private void subpingPeer(NodeState dst) {
        if (dst.info.id != myNid && dst.hop != 0) {
            NodeState hop = nodes.get(dst.hop);
            long time = System.currentTimeMillis();
            InetSocketAddress nod = new InetSocketAddress(dst.info.addr,
                    dst.info.port);
            InetSocketAddress hopAddr = new InetSocketAddress(
                    hop.info.addr, hop.info.port);
            subprobeBytes += sendObj(subprobe(nod, time, SUBPING), hopAddr);
            subprobeCount++;
        }
    }

    private final Serialization probeSer = new Serialization();

    private int sendObj(Object o, InetSocketAddress dst) {
        try {
            // TODO investigate directly writing to ByteBuffer
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            probeSer.serialize(o, new DataOutputStream(baos));
            byte[] buf = baos.toByteArray();
            session.send(ByteBuffer.wrap(buf), dst);
            return buf.length;
        } catch (Exception ex) {
            err(ex);
            return 0;
        }
    }

    private void handleSubping(Subprobe p) {
        if (myAddr.equals(p.nod)) {
            sendObj(subprobe(p.nod, p.time, SUBPONG_FWD), p.src);
            log("subprobe", "direct subpong from/to " + p.src);
        } else {
            // we also checked for p.src because eventually we'll need to
            // forward the subpong back too; if we don't know him, no point in
            // sending a subping
            sendObj(subprobe(p.src, p.time, SUBPING_FWD), p.nod);
//            log("subprobe", "subping fwd from " + p.src + " to " + p.nod);
        }
    }

    private void handleSubpingFwd(Subprobe p) {
        sendObj(subprobe(p.nod, p.time, SUBPONG), p.src);
//        log("subprobe", "subpong to " + p.nod + " via " + p.src);
    }

    private void handleSubpong(Subprobe p) {
        sendObj(subprobe(p.src, p.time, SUBPONG_FWD), p.nod);
//        log("subprobe", "subpong fwd from " + p.src + " to " + p.nod);
    }

    private final Hashtable<InetSocketAddress, NodeState> addr2node = new Hashtable<InetSocketAddress, NodeState>();

    private int addr2nid(InetSocketAddress a) {
        return addr2node.get(a).info.id;
    }

    private void handleSubpongFwd(Subprobe p, long receiveTime) {
        long latency = (receiveTime - p.time) / 2;
        log("subpong from " + addr2nid(p.nod) + " via " + addr2nid(p.src) + ": " + latency + ", time " + p.time);
    }

    private final int fastProbePeriodMs = 3000;


    private long pingPeer(NodeInfo info) {
        PeerPing ping = new PeerPing();
        ping.time = System.currentTimeMillis();
        ping.src = myAddr;
        pingpongBytes += sendObj(ping, new InetSocketAddress(info.addr,
                info.port));
        pingpongCount++;
        return ping.time;
    }

    private void pingCoord() {
        Ping p = new Ping();
        p.time = System.currentTimeMillis();
        NodeInfo tmp = nodes.get(myNid).info;
        p.info = new NodeInfo();
        p.info.id = origNid; // note that the ping info uses the original id
        p.info.addr = tmp.addr;
        p.info.port = tmp.port;

        pingpongBytes += sendObject(p, (short) 0);
        pingpongCount++;
    }

    private Object deserialize(ByteBuffer buf) {
        byte[] bytes = new byte[buf.limit()];
        buf.get(bytes);
        try {
            return new Serialization().deserialize(new DataInputStream(new
                        ByteArrayInputStream(bytes)));
        } catch (Exception ex) {
            err(ex);
            return null;
        }
    }

    private Hashtable<Short,Short> id2oid = new Hashtable<Short,Short>();
    private Hashtable<Short,String> id2name = new Hashtable<Short,String>();

    /**
     * coordinator's msg handling loop
     */
    public final class CoordHandler implements ReactorHandler {

        /**
         * Generates non-repeating random sequence of short IDs, and keeps
         * track of how many are emitted.
         */
        public final class IdGenerator {
            private final Iterator<Short> iter;
            private short counter;
            public IdGenerator() {
                List<Short> list = new ArrayList<Short>();
                for (short s = 1; s < Short.MAX_VALUE; s++) {
                    list.add(s);
                }
                Collections.shuffle(list);
                iter = list.iterator();
            }
            public short next() {
                counter++;
                return iter.next();
            }
            public short count() {
                return counter;
            }
        }
        private IdGenerator nidGen = new IdGenerator();

        private void sendInit(short nid, Join join) {
            Init im = new Init();
            im.id = nid;
            im.members = getMemberInfos();
            sendObject(im, join.addr, join.port, (short)-1);
        }

        @Override
        public void handle(Session session, InetSocketAddress src, java.nio.ByteBuffer buf) {
            try {
                Msg msg = (Msg) deserialize(buf);
                if (msg == null) return;
                if (msg.session == sessionId) {
                    if (msg instanceof Join) {
                        final Join join = (Join) msg ;
                        if (id2oid.values().contains(msg.src)) {
                            // we already added this guy; just resend him the init msg
                            sendInit(addr2id.get(join.addr), join);
                        } else {
                            // need to add this guy and send him the init msg (if there's space)
                            if (!capJoins || coordNodes.size() < numNodesHint) {
                                short newNid = nidGen.next();
                                addMember(newNid, join.addr, join.port, join.src);
                                if (blockJoins) {
                                    if (coordNodes.size() >= numNodesHint) {
                                        // time to broadcast ims to everyone
                                        ArrayList<NodeInfo> memberList = getMemberInfos();
                                        for (NodeInfo m : memberList) {
                                            Init im = new Init();
                                            im.id = m.id;
                                            im.members = memberList;
                                            sendObject(im, m);
                                        }
                                    }
                                } else {
                                    sendInit(newNid, join);
                                    broadcastMembershipChange(newNid);
                                }

                                if (coordNodes.size() == numNodesHint) {
                                    semAllJoined.run();
                                }
                            } else if (capJoins && coordNodes.size() == numNodesHint) {
                                Init im = new Init();
                                im.id = -1;
                                im.members = new ArrayList<NodeInfo>();
                                sendObject(im, join.addr, join.port, (short)-1);
                            }
                        }
                    } else if (coordNodes.containsKey(msg.src)) {
                        log("recv." + msg.getClass().getSimpleName(), "from " +
                                msg.src + " (oid " + id2oid.get(msg.src) + ", "
                                + id2name.get(msg.src) + ")");
                        resetTimeoutAtCoord(msg.src);
                        if (msg.version < currentStateVersion) {
                            // this includes joins
                            log("updating stale membership");
                            sendMembership(msg.src);
                        } else if (msg instanceof Ping) {
                            // ignore the ping
                        } else {
                            throw new Exception("can't handle message type here: " + msg.getClass().getName());
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
                                nid = nidGen.next();
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
            } catch (Exception ex) {
                err(ex);
            }
        }
    }


    /**
     * coordinator's msg handling loop
     */
    public final class CoordReceiver extends IoHandlerAdapter {
        @Override
        public void messageReceived(IoSession session, Object obj)
                throws Exception {
            assert false;
        }
    }

    private int pingpongCount, pingpongBytes, subprobeCount, subprobeBytes;

    public final class NodeHandler implements ReactorHandler {

        public short getSimLatency(short nid) {
            long time = System.currentTimeMillis();
            for (SimEvent e : simEvents) {
                if (time - startTime >= e.secs * 1000) {
                    // make this event happen
                    simLatencies.put(e.dst, e.lat);
                }
            }
            return simLatencies.get(nid);
        }

        @Override
        public void handle(Session session, InetSocketAddress src, ByteBuffer buf) {
            try {
                // TODO check SimpleMsg.session
                Object obj = deserialize(buf);
                if (obj == null) return;
                long receiveTime;
                if (obj instanceof Subprobe) {
                    Subprobe p = (Subprobe) obj;
                    subprobeBytes += buf.limit();
                    subprobeCount += 2;
                    switch (p.type) {
                        case SUBPING: handleSubping(p); break;
                        case SUBPING_FWD: handleSubpingFwd(p); break;
                        case SUBPONG: handleSubpong(p); break;
                        case SUBPONG_FWD:
                        // TODO move into the new worker thread when it's here
                        receiveTime = System.currentTimeMillis();
                        handleSubpongFwd(p, receiveTime);
                        break;
                        default: assert false;
                    }
                    return; // TODO early exit is unclean
                } else if (obj instanceof PeerPing) {
                    PeerPing ping = ((PeerPing) obj);
                    PeerPong pong = new PeerPong();
                    pong.time = ping.time;
                    pong.src = myAddr;
                    pingpongBytes += sendObj(pong, ping.src) + buf.limit();
                    pingpongCount += 2;
                    return; // TODO early exit is unclean
                } else if (obj instanceof PeerPong) {
                    PeerPong pong = (PeerPong) obj;
                    long rawRtt = System.currentTimeMillis() - pong.time;
                    if (mode == RunMode.SIM) {
                        short l = getSimLatency(addr2node.get(pong.src).info.id);
                        if (l < resetLatency) {
                            rawRtt = 2 * l;
                        }
                    }
                    NodeState state = addr2node.get(pong.src);
                    // if the rtt was astronomical, just treat it as a dropped packet
                    if (rawRtt / 2 < Short.MAX_VALUE) {
                        // we define "latency" as rtt/2; this should be
                        // a bigger point near the top of this file
                        short latency = (short) (rawRtt / 2);
                        short nid = state.info.id;
                        if (state != null) {
                            resetTimeoutAtNode(nid, pong.time);
                            NodeState self = nodes.get(myNid);
                            short oldLatency = self.latencies.get(nid);
                            final short ewma;
                            if (oldLatency == resetLatency) {
                                ewma = latency;
                            } else {
                                ewma = (short) (smoothingFactor * latency +
                                        (1 - smoothingFactor) * oldLatency);
                            }
                            log("latency", state + " = " + latency +
                                    ", ewma " + ewma + ", time " +
                                    pong.time);
                            self.latencies.put(nid, ewma);
                        } else {
                            log("latency", "some " + nid + " = " + latency);
                        }
                        pingpongCount++;
                        pingpongBytes += buf.limit();
                    }
                    return; // TODO early exit is unclean
                }
                Msg msg = (Msg) obj;
                if ((msg.src == 0 || nodes.containsKey(msg.src) || msg instanceof Ping)
                        && msg.session == sessionId) {
                    NodeState state = nodes.get(msg.src);

                    log("recv." + msg.getClass().getSimpleName(), "from "
                            + msg.src + " len " + ((ByteBuffer) buf).limit());

                    // for other messages, make sure their state version is
                    // the same as ours

                    if (msg.version > currentStateVersion) {
                        if (msg instanceof Membership && hasJoined) {
                            currentStateVersion = msg.version;
                            Membership m = (Membership) msg;
                            assert myNid == m.yourId;
                            updateMembers(m.members);
                        } else if (msg instanceof Init) {
                            hasJoined = true;
                            if (semAllJoined != null)
                                semAllJoined.run();
                            if (((Init) msg).id == -1)
                                session.close();
                            handleInit((Init) msg);
                        } else {
                            // i am out of date - wait until i am updated
                        }
                    } else if (msg.version == currentStateVersion) {
                        // from coordinator
                        if (msg instanceof Membership) {
                            Membership m = (Membership) msg;
                            assert myNid == m.yourId;
                            updateMembers(m.members);
                        } else if (msg instanceof Measurements) {
                            resetTimeoutOnRendezvousClient(msg.src);
                            updateMeasurements((Measurements) msg);
                        } else if (msg instanceof RoutingRecs) {
                            RoutingRecs recs = (RoutingRecs) msg;
                            handleRecommendations(recs);
                            log("got recs " + routesToString(recs.recs));
                        } else if (msg instanceof Ping ||
                                   msg instanceof Pong ||
                                   msg instanceof Init) {
                            // nothing to do, already handled above
                        } else {
                            throw new Exception("can't handle that message type");
                        }
                    } else {
                        log("stale." + msg.getClass().getSimpleName(),
                                "from " + msg.src + " version "
                                        + msg.version + " current "
                                        + currentStateVersion);
                    }
                } else {
                    // log("ignored." + msg.getClass().getSimpleName(), "ignored from " + msg.src + " session " + msg.session);
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
        public void messageReceived(IoSession session, Object buf)
                throws Exception {
            assert false;
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

    private final Hashtable<Short, ScheduledFuture<?>> rendezvousClientTimeouts = new Hashtable<Short, ScheduledFuture<?>>();

    private void resetTimeoutOnRendezvousClient(final short nid) {
        final NodeState node = nodes.get(nid);
	// TODO: wrong semantics for isReachable
        if (!node.isReachable) return;

        ScheduledFuture<?> oldFuture = rendezvousClientTimeouts.get(nid);
        if (oldFuture != null) {
            oldFuture.cancel(false);
        }

        if (rendezvousClients.add(node)) {
            log("rendezvous client " + node + " added");
        }

        ScheduledFuture<?> future = scheduler.schedule(safeRun(new Runnable() {
            public void run() {
                if (rendezvousClients.remove(node)) {
                    log("rendezvous client " + node + " removed");
                }
            }
        }), clientTimeout, TimeUnit.SECONDS);
        rendezvousClientTimeouts.put(nid, future);
    }

    private void resetTimeoutAtNode(final short nid, long timestamp) {
        if (nodes.containsKey(nid)) {
            final NodeState node = nodes.get(nid);
            if (!node.isReachable) {
                log(nid + " reachable");
                if (node.hop == 0) {
                    node.isHopRecommended = false;
                    node.cameUp = true;
                    node.hop = nid;
                }
            }
            node.isReachable = true;
            ProbeTask task = probeFutures.get(nid);
            if (task != null && timestamp == task.timestamp) {
		// We finally got a pong for the node, so reset the number of
		// consecutive probes.
		task.consecutiveUnansweredProbes = 0;
                task.schedule(probePeriod * 1000, true);
            }
	    node.isDead = false;
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
        id2oid.put(newNid, origId);
        id2name.put(newNid, addr.getHostName());
        addr2id.put(addr, newNid);
        log("adding new node: " + newNid + " oid " + origId + " name " +
                id2name.get(newNid));
        currentStateVersion++;
        resetTimeoutAtCoord(newNid);
        return info;
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
            for (short nid : coordNodes.keySet()) {
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
        sendObject(msg, coordNodes.get(nid));
    }

    /**
     * a coordinator-only method
     *
     * NOTE: there is a hack workaround here for sim mode, because addr2id is
     * not storing unique host:port combos, only unique hosts.
     *
     * @param nid
     */
    private void removeMember(short nid) {
        log("removing dead node " + nid + " oid " + id2oid.get(nid) + " " +
                id2name.get(nid));
        id2oid.remove(nid);
        NodeInfo info = coordNodes.remove(nid);
        Short mid = addr2id.remove(info.addr);
        if (mode != RunMode.SIM)
            assert mid != null;
        currentStateVersion++;
        broadcastMembershipChange(nid);
    }

    private final class ProbeTask implements Runnable {
        public long timestamp;
        /**
         * When we were scheduled to run in the previous round.
         */
        public long lastScheduledTime;
        public long nextScheduledTime;
        public ScheduledFuture<?> future;
        public final NodeState node;
        public int consecutiveUnansweredProbes = 0;

        public ProbeTask(NodeState n) {
            node = n;
        }

        /**
         * This will set the scheduled wake-up time to be lastScheduledTime +
         * delay. So, you may call this multiple times to schedule regular times
         * (until time reaches nextScheduledTime). And jitter is counted only in
         * the actual schedule, but does not influencing the rhythm. A bit
         * confusing.
         *
         * @param delay
         *                milliseconds
         */
        public void schedule(long delay, boolean doJitter) {
            if (future == null)
                lastScheduledTime = System.currentTimeMillis();
            nextScheduledTime = lastScheduledTime + delay;

            System.out.println("bumping by " + delay);
            if (future != null)
                future.cancel(true);
            int jitter = doJitter ? rand.nextInt(2000) - 1000 : 0;
            future = scheduler.schedule(this, jitter + nextScheduledTime
                    - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        public void run() {

	    // If we don't receive a pong after 5 consecutive fast probes
	    // we conclude that the node is unreachable.
	    if(consecutiveUnansweredProbes == 5) {

		log(myNid + " unreachable");
		node.isReachable = false;
		nodes.get(myNid).latencies.remove(node.info.id);

		// TODO: This logic needs to be changed, as it should
		//       not be coupled with our measurement data. For
		//       example, it is important that we quickly
		//       report reachability failures in our measurements,
		//       so that other nodes can judge node failures.
		//       However, it doesn't necessarily hurt to continue sending
		//       recommendations *for* (but not to) nodes from whom
		//       we can't receive measurement packets (up to some amount
		//       of time, of course!). Imagine scenarios where the
		//       client has a very high loss rate to its rendezvous
		//       servers. We rather compute optimal one-hops for it
		//       using slightly stale data (but new data from other
		//       nodes) than not at all. However, it is important in
		//       this setting that we associate a timestamp with each
		//       recommendation which says how old the measurements are
		//       that were used to compute the recommendation.
		rendezvousClients.remove(node);

		findPaths(node, false);
	    }
	    else {

		System.out.println(myNid + " pinging " + node.info.id);
		timestamp = pingPeer(node.info);

		consecutiveUnansweredProbes++;
		System.out.println("#consecutiveUnansweredProbes = " + consecutiveUnansweredProbes);
	    }


            // On the fifth probe, slow back down. We don't reset the consecutiveUnansweredProbes
	    // counter until we successfully receive a pong. Thus, after the initial burst
	    // of 5 probes, we will consider a node dead and will only bother pinging it
	    // once every 30 seconds until it comes up again (by successfully responding to
	    // one of these pings). If we think failures are short lived, but the loss rate
	    // is high, we might want to consider a different design choice.
            int nextDelay = consecutiveUnansweredProbes >= 5 ? probePeriod * 1000
                    : fastProbePeriodMs;
            lastScheduledTime = nextScheduledTime;
            schedule(nextDelay, true);
        }
    }

    private final Hashtable<Short, ProbeTask> probeFutures = new Hashtable<Short, ProbeTask>();
    private final Hashtable<Short, ScheduledFuture<?>> subprobeFutures = new Hashtable<Short, ScheduledFuture<?>>();

    /**
     * updates our member state. modifies data structures as necessary to
     * maintain invariants.
     *
     * @param newNodes
     */
    private void updateMembers(List<NodeInfo> newNodes) {

	HashSet<Short> newNids = new HashSet<Short>();
	for (NodeInfo node : newNodes)
	    newNids.add(node.id);

	    // add new nodes

	    for (final NodeInfo node : newNodes)
		if (!nodes.containsKey(node.id)) {

		    final NodeState newNode = new NodeState(node);

                    if (node.id != myNid) {
                        // probes
                        ProbeTask task = new ProbeTask(newNode);
                        probeFutures.put(node.id, task);
                        task.schedule(rand.nextInt(30000), false);

                        // subprobes
                        scheduler.schedule(new Runnable() {
                            public void run() {
                                subpingPeer(newNode);
                                int jitter = rand.nextInt(2000) - 1000;
                                subprobeFutures.put(node.id, scheduler.schedule(this, subpingPeriod * 1000
                                        + jitter, TimeUnit.MILLISECONDS));
                            }
                        }, rand.nextInt(30000), TimeUnit.MILLISECONDS);
                    }

		    // Choose a subinterval for this node during which we will ping it
//		    int loc = rand.nextInt(numProbeIntervals);
//		    pingTable[loc].add(newNode);
//		    pingId.put(newNode, loc);

		    nodes.put(node.id, newNode);
                    addr2node.put(new InetSocketAddress(node.addr, node.port), newNode);
		    if (node.id != myNid)
			resetTimeoutAtNode(node.id, -1);
		}

	    // Remove nodes. We need toRemove to avoid
        // ConcurrentModificationException on the table that we'd be looping
        // through.

        for (NodeInfo node : newNodes)
            newNids.add(node.id);
        HashSet<Pair<Short, NodeState>> toRemove = new HashSet<Pair<Short,NodeState>>();
        for (Map.Entry<Short, NodeState> entry : nodes.entrySet())
            if (!newNids.contains(entry.getKey()))
                toRemove.add(Pair.of(entry.getKey(), entry.getValue()));
        for (Pair<Short, NodeState> pair : toRemove) {
            short nid = pair.first;
            NodeState node = pair.second;
            // Remove the node from the subinterval during which it
            // was pinged.
//            int index = pingId.remove(node);
//            pingTable[index].remove(node);
            probeFutures.remove(nid).future.cancel(false);
            subprobeFutures.remove(nid).cancel(false);
            addr2node.remove(new InetSocketAddress(node.info.addr, node.info.port));
            NodeState n = nodes.remove(nid);
            assert n != null;
        }


        // consistency cleanups: check that all nid references are still valid nid's

        for (NodeState state : nodes.values()) {
            if (state.hop != 0 && !newNids.contains(state.hop)) {
                state.hop = state.info.id;
                state.isHopRecommended = false;
            }

            for (Iterator<Short> i = state.hopOptions.iterator(); i.hasNext();)
                if (!newNids.contains(i.next()))
                    i.remove();

            HashSet<Short> garbage = new HashSet<Short>();
            for (short nid : state.latencies.keySet())
                if (!newNids.contains(nid))
                    garbage.add(nid);
            for (short nid : garbage)
                state.latencies.remove(nid);

	    // Clear the remote failures hash, since this node will now have a different
	    // set of default rendezvous nodes.
	    state.remoteFailures.clear();
        }

        //
        // regenerate alternative views of this data
        //

        NodeState self = nodes.get(myNid);
        assert self != null;

        memberNids.clear();
        memberNids.addAll(newNids);
        Collections.sort(memberNids);

        otherNodes.clear();
        otherNodes.addAll(nodes.values());
        otherNodes.remove(self);

	// ABOVE IS INDEPENDENT OF GRID

	// numRows needs to be >= numCols
        numRows = (short) Math.ceil(Math.sqrt(nodes.size()));
        numCols = (short) Math.ceil((double) nodes.size() / (double) numRows);

        grid = new NodeState[numRows][numCols];

	// These are used temporarily for setting up the defaults
	Hashtable<NodeState, Short> gridRow = new Hashtable<NodeState, Short>();
	Hashtable<NodeState, Short> gridColumn = new Hashtable<NodeState, Short>();

        List<Short> nids = memberNids;
	short i = 0;  // node counter
	short numberOfNodes = (short) memberNids.size();
	short lastColUsed = (short) (numCols - 1);  // default is for the full grid

	gridLoop:
	for (short r = 0; r < numRows; r++) {
            for (short c = 0; c < numCols; c++) {

		// Are there any more nodes to put into the grid?
		if(i > numberOfNodes - 1) {
		    // Assert: We shouldn't create a grid with an empty last row.
		    assert (r == numRows - 1) && (c > 0);
		    lastColUsed = (short) (c - 1);
		    break gridLoop;
		}

                grid[r][c] = nodes.get(nids.get(i++));
                gridRow.put(grid[r][c], r);
                gridColumn.put(grid[r][c], c);
            }
	}

	// Algorithm described in model_choices.tex

	// Set up hash of each node's default rendezvous servers
	// Note: a node's default rendezvous servers will include itself.
	nodeDefaultRSs.clear();
	for(NodeState node : nodes.values()) {

	    int rn = gridRow.get(node);
	    int cn = gridColumn.get(node);

	    // We know the number of elements. Should be [1/(default load factor)]*size
	    HashSet<NodeState> nodeDefaults = new HashSet<NodeState>((int) 1.4*(numRows + numCols - 1));

	    // If this is not the last row
	    if(rn < numRows - 1) {

		// Add the whole row
		for (int c1 = 0; c1 < numCols; c1++)
		    nodeDefaults.add(grid[rn][c1]);

		// If this is before the last col used (on last row)
		if(cn <= lastColUsed) {

		    // Add whole column
		    for (int r1 = 0; r1 < numRows; r1++)
			nodeDefaults.add(grid[r1][cn]);
		}
		else {

		    // Add column up until last row
		    for (int r1 = 0; r1 < numRows-1; r1++)
			nodeDefaults.add(grid[r1][cn]);

		    // Add corresponding node from the last row (column rn);
		    //    only for the first lastColUsed rows.
		    if(rn <= lastColUsed) {
			nodeDefaults.add(grid[numRows-1][rn]);
		    }
		}
	    }
	    else {

		// This is the last row

		// Add whole column
		for (int r1 = 0; r1 < numRows; r1++)
		    nodeDefaults.add(grid[r1][cn]);

		// Add whole last row up till lastColUsed
		for (int c1 = 0; c1 <= lastColUsed; c1++)
		    nodeDefaults.add(grid[rn][c1]);

		// Add row cn for columns > lastColUsed
		for (int c1 = lastColUsed+1; c1 < numCols; c1++)
		    nodeDefaults.add(grid[cn][c1]);
	    }

	    // Could also make an array of nodeDefaults, for less memory usage/faster
	    nodeDefaultRSs.put(node, nodeDefaults);
	}

	// BELOW IS INDEPENDENT OF GRID

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

	// Set up rendezvous clients
        rendezvousClients.clear();
        for (NodeState cli : nodeDefaultRSs.get(self)) {

	    // TODO: wrong semantics for isReachable
            if (cli.isReachable && cli != self)
		rendezvousClients.add(cli);
	}

        // Put timeouts for all new rendezvous clients. If they can never
        // reach us, we should stop sending them recommendations.
        for (final NodeState clientNode : rendezvousClients) {
            ScheduledFuture<?> oldFuture = rendezvousClientTimeouts.get(clientNode.info.id);
            if (oldFuture != null) {
                oldFuture.cancel(false);
            }
            ScheduledFuture<?> future = scheduler.schedule(safeRun(new Runnable() {
                public void run() {
                    if (rendezvousClients.remove(clientNode)) {
                        log("rendezvous client " + clientNode + " removed");
                    }
                }
            }), clientTimeout, TimeUnit.SECONDS);
            rendezvousClientTimeouts.put(clientNode.info.id, future);
        }

        // Set up default rendezvous servers to all destinations

	// Note: In an earlier version of the code, for a destination in
	//       our row/col, we did not add rendezvous nodes which are not
	//       reachable. We no longer do this (but it shouldn't matter).

        defaultRendezvousServers.clear();
        for (NodeState dst : nodes.values()) {   // note: including self

	    HashSet<NodeState> rs = new HashSet<NodeState>();
	    defaultRendezvousServers.put(dst.info.id, rs);

	    // Take intersection of this node's default rendezvous and
	    // the dst's default rendezvous servers, excluding self.
	    // Running time for outer loop is 2n^{1.5} since we are using
	    // a HashSet for quick lookups. Could be optimized further,
	    // but code simplicity is preferred.

	    HashSet<NodeState> dstDefaults = nodeDefaultRSs.get(dst);
	    for (NodeState selfRS : nodeDefaultRSs.get(self)) {

		// Don't add self because we will never receive routing
		// recommendation messages from ourselves.
		if (selfRS != self && dstDefaults.contains(selfRS))
		    rs.add(selfRS);
	    }
	}

	// Create empty set for default rendezvous servers, will be filled in
	// getAllRendezvousServers()
        rendezvousServers.clear();
        for (Entry<Short, HashSet<NodeState>> entry : defaultRendezvousServers.entrySet()) {
            rendezvousServers.put(entry.getKey(), new HashSet<NodeState>());
        }
        lastRendezvousServers.clear();

        log("state " + currentStateVersion + ", mbrs " + nids);
    }

    /**
     * @param n
     * @param remoteNid
     * @return
     */
    private boolean isFailedRendezvous(NodeState n, NodeState remote) {
	// TODO: isReachable semantics should be fixed (but how?)
	// NOTE: We may never receive this node's measurements since
	//       it is our rendezvous client, but we don't offer to be
	//       its rendezvous server. This is why we check for
	//       remote in the recommendations response rather than
	//       the measurements.
	//       This assumes that the node's reachability is indicative
	//       of its ability to send us recommendation messages.
        return !n.isReachable || n.remoteFailures.contains(remote);
    }

    /**
     * @return failoverClients `union` nodes in my row and col (wherever i occur)
     */
    private ArrayList<NodeState> getAllRendezvousClients() {
        ArrayList<NodeState> list = new ArrayList<NodeState>(rendezvousClients);
        Collections.sort(list);
        return list;
    }

    private String mkString(HashSet<NodeState> ns, String glue) {
        String s = "";
        for (NodeState n : ns) {
            s += n.info.id + glue;
        }
        return s;
    }

    /**
     * makes one pass over the metaset of all rendezvous servers, removing any
     * failed rendezvous from the individual sets.
     *
     * for the simple routing scheme, this is the full set of nodes. as a
     * result, measurements are broadcast to everyone, as intended. (note that
     * there are no routing recommendation messages in this scheme.)
     *
     * OLD ALGO
     *
     * for dst
     *   if dst is not down
     *     rs = dst's current rendezvous servers
     *     ds = dst's default rendezvous servers
     *     if any of ds are working to dst and rs is not ds
     *       rs = working subset of ds
     *     if rs = []
     *       rs += random node from dst's row/col
     *     else
     *       rs -= any failed rs
     *       note that this may leave rs empty for the coming round
     *       this is what we want bc it will delay failover-finding till the next round
     *
     * NEW ALGO
     *
     * // CHANGES START
     * build a hashtable for all rendezvous nodes currently used
     * call this F
     * // CHANGES END
     * for dst
     *   if dst is not down
     *     rs = dst's current rendezvous servers
     *     ds = dst's default rendezvous servers
     *     if any of ds are working to dst and rs is not ds
     *       rs = working subset of ds
     *     if rs = []
     *       // CHANGES START
     *       for active failover in dst's default rendezvous nodes (according to F)
     *         if failover works to dst, choose it as failover for dst as well
     *       choose rand node from dst's default rendezvous nodes that is not currently in use
     *       rs += whatever we chose; F += whatever we chose
     *       // CHANGES END
     *     else
     *       rs -= any failed rs
     *       note that this may leave rs empty for the coming round
     *       this is what we want bc it will delay failover-finding till the next round
     *
     * @return the union of all the sets of non-failed rendezvous servers.
     */
    private ArrayList<NodeState> getAllRendezvousServers() {

        NodeState self = nodes.get(myNid);
        HashSet<NodeState> currentRSs = new HashSet<NodeState>();
        HashSet<NodeState> allDefaults = nodeDefaultRSs.get(self);

        // first, prepare currentRS so that we can share/reuse
        // rendezvous servers
        for (NodeState node : otherNodes) {
	    for (NodeState n : rendezvousServers.get(node.info.id)) {
		// if this is an actual failover
		if (!allDefaults.contains(n))
		    currentRSs.add(n);
	    }
	}

        // these are the rendezvous servers that we want to sent our
        // measurements to
        HashSet<NodeState> servers = new HashSet<NodeState>();

	// iterate over all destination nodes that are not us
        for (NodeState dst : otherNodes) {

	    if (!dst.isDead) {
		// this is our current (active) set of rendezvous servers
		HashSet<NodeState> rs = rendezvousServers.get(dst.info.id);

		// check if any of our default rendezvous servers are once
		// more available; if so, add them back
		HashSet<NodeState> defaults = defaultRendezvousServers.get(dst.info.id);

		// we always want to try talking to our default rendezvous
		// servers if we think they're reachable
		for (NodeState r : defaults)
		    if (r.isReachable)
			servers.add(r);

		// rs consists of either default rendezvous servers or
		// non-default rendezvous, but never a mix of both; test
		// which type it is
		boolean hasDefaultsOnly = rs.isEmpty() ?
		    true : defaults.contains(rs.iterator().next());

		// the following code attempts to add default rendezvous
		// servers back into rs
		HashSet<NodeState> old = new HashSet<NodeState>(rs);
		if (hasDefaultsOnly) {
		    // if any default rendezvous servers are in use, then
		    // don't clear rs; simply add any more default servers
		    // that are working
		    if (!defaults.equals(rs))
			for (NodeState r : defaults)
			    if (!isFailedRendezvous(r, dst))
				rs.add(r);
		} else {
		    // if no default rendezvous servers are in use, then
		    // try adding any that are working; if any are working,
		    // we make sure to first clear rs
		    boolean cleared = false;
		    for (NodeState r : defaults) {
			if (!isFailedRendezvous(r, dst)) {
			    if (!cleared) {
				rs.clear();
				cleared = true;
			    }
			    rs.add(r);
			}
		    }
		}
		if (!old.equals(rs)) {
		    log("restored rendezvous server for " + dst + " from " + old + " to " + rs);
		}

		// Note that rs not being empty means that in previous iterations the nodes in
		// rs were alive, and either we did not ever receive any recommendations from them,
		// or the last recommendation we received from it did include routing information
		// for dst (and so there is no hint of a remote failure). In either case, as of
		// the previous iteration, n.remoteFailures.contains(remote) == false.

		// if { any node in rs has n.remoteFailures.contains(remote) == true, then we know that we
		//   did receive a routing recommendation from it since the last round, and it is alive.
		//   Remove it from rs and do nothing else this step, as the destination is likely dead.
		//   set skipIteration=true. }
		// else {
		//   If !n.isReachable for any node n in rs, remove it from rs. We don't expect it to be
		//     helpful for routing ever.
		//   If rs is now empty, choose a failover rendezvous node (in this iteration)
		//   Else, any remaining nodes have n.remoteFailures.contains(remote) == false, which means
		//     either that we did not yet receive a routing message from it, or we did and the dst
		//     is reachable. In either case, do nothing. If this node is still alive, we will
		//     eventually receive a routing recommendation from it. Otherwise, very soon we will find
		//     that !n.isReachable. We add a bit of latency for waiting, but should be okay.
		// }

		/*
		 * If we think a remote failure could have occured, don't immediately look
		 * for failovers. The next period, we will have received link states from
		 * our neighbors, from which we can determine whether dst is just down.
		 *
		 * The reason for this is that if a node fails, we don't want the entire network to flood
		 * the row/col of that downed node (no need for failovers period).
		 */

		boolean skipIteration = false;

		// We use the iterator so that we can safely remove from the set
		for (Iterator<NodeState> i = rs.iterator(); i.hasNext();) {
		    NodeState r = i.next();

		    if(r.remoteFailures.contains(dst)) {
			i.remove();
			skipIteration = true;
		    }
		    else if(!r.isReachable) {
			i.remove();
		    }
		}


		if (!skipIteration && rs.isEmpty() && scheme != RoutingScheme.SQRT_NOFAILOVER) {
		    // create debug string
		    String s = "defaults";
		    for (NodeState r : defaults) {
			s += " " + r.info.id + (
						!r.isReachable ? " unreachable" :
						" <-/-> " + mkString(r.remoteFailures, ",") );
		    }
		    final String report = s;

		    // look for failovers

		    HashSet<NodeState> cands = new HashSet<NodeState>();

		    // first, start by looking at the failovers that are
		    // currently in use which are default rs's for this dst, so
		    // that we can share when possible. that is, if we know that a
		    // failover works for a destination, keep using it.

		    HashSet<NodeState> dstDefault = nodeDefaultRSs.get(dst);

		    // currentRSs may contain rendezvous nodes which are no longer alive
		    // or useful for reaching the destination
		    for(NodeState f : currentRSs) {
			if (dstDefault.contains(f) && !isFailedRendezvous(f, dst)) {
			    cands.add(f);
			}
		    }

		    if (cands.isEmpty()) {

			// only once we have determined that no current
			// failover works for us do we go ahead and randomly
			// select a new failover. this is a blind choice;
			// we don't have these node's routing recommendations,
			// so we could not hope to do better.
			// TODO (low priority): one exception is if any of the candidates
			// are rendezvous clients for us, in which case we
			// will have received their link state, and we could
			// smartly decide whether they can reach the destination.
			// Not obvious if we should (or how) take advantage of this.

			for(NodeState cand : dstDefault) {
			    if (cand != self && cand.isReachable)
				cands.add(cand);
			}
		    }

		    // if any of the candidates are already selected to be in
		    // 'servers', we want to make sure that we only choose from
		    // these choices.
		    HashSet<NodeState> candsInServers = new HashSet<NodeState>();
		    for (NodeState cand : cands)
			if (servers.contains(cand))
			    candsInServers.add(cand);

		    // choose candidate uniformly at random
		    ArrayList<NodeState> candsList = new
			ArrayList<NodeState>(candsInServers.isEmpty() ?
					     cands : candsInServers);
		    if (candsList.size() == 0) {
			log("no failover candidates! giving up; " + report);
		    } else {
			NodeState failover = candsList.get(rand.nextInt(candsList.size()));
			// TODO (low priority): prev rs = ... is now broken since rs is empty
			log("new failover for " + dst + ": " + failover + ", prev rs = " + rs + "; " + report);
			rs.add(failover);

			// share this failover in this routing iteration too
			if (!allDefaults.contains(failover)) {
			    currentRSs.add(failover);
			}
		    }
		}
		else if (rs.isEmpty()) {
		    log("all rs to " + dst + " failed");
		    System.out.println("ALL FAILED!");
		}

		// Add any nodes that are in rs to the servers set
		for (NodeState r : rs) {
		    servers.add(r);
		}

	    } // end if dst.hop != 0 (destination is alive)
        } // end while loop over destinations

        ArrayList<NodeState> list = new ArrayList<NodeState>(servers);
        Collections.sort(list);
        return list;
    }

    public static enum RoutingScheme { SIMPLE, SQRT, SQRT_NOFAILOVER, SQRT_RC_FAILOVER, SQRT_SPECIAL };
    private final RoutingScheme scheme;

    private void printMembers() {
        String s = "members:";
        for (NodeInfo info : coordNodes.values()) {
            s += "\n  " + info.id + " oid " + id2oid.get(info.id) + " " +
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
        ArrayList<NodeState> dsts = new ArrayList<NodeState>(clients);
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

    /**
     * Given the src, find for each dst in dsts the ideal hop from hops,
     * storing these into recs.  This may choose the dst itself as the hop.
     */
    private void findHops(ArrayList<NodeState> dsts,
            ArrayList<Short> hops, NodeState src, ArrayList<Rec> recs) {
        for (NodeState dst : dsts) {
            if (src != dst) {
                short min = resetLatency;
                short minhop = -1;
                for (short hop : hops) {
                    if (hop != src.info.id) {
                        int src2hop = src.latencies.get(hop);
                        int dst2hop = dst.latencies.get(hop);
                        int latency = src2hop + dst2hop;
                        // DEBUG
                        // log(src + "->" + hop + " is " + src2hop + ", " + hop +
                        //         "->" + dst + " is " + dst2hop + ", sum " +
                        //         latency);
                        if (latency < min) {
                            min = (short) latency;
                            minhop = hop;
                        }
                    }
                }

                // it's possible for us to have not found an ideal hop. this
                // may be counter-intuitive, since even if src<->dst is broken,
                // the fact that both are our clients means we should be able
                // to serve as a hop. however it may be that either one of them
                // was, or we were, recently added as a member to the network,
                // so they never had a chance to ping us yet (and hence we're
                // missing from their measurements). (TODO also is it possible
                // that we're missing their measurement entirely? are all
                // clients necessarily added on demand by measurement packets?)
                // what we can do is to try finding our own latency to the hop
                // (perhaps we've had a chance to ping them), and failing that,
                // estimating the latency (only if one of us was newly added).
                // however, these errors are transient anyway - by the next
                // routing period, several pings will have taken place that
                // would guarantee (or there was a failure, and eventually one
                // of {src,dst} will fall out of our client set).
                if (minhop != -1) {
                    short directLatency = src.latencies.get(dst.info.id);
                    Rec rec = new Rec();
                    rec.dst = dst.info.id;
                    // We require that a recommended route (if not the direct
                    // route and if direct route is working) yield at least a
                    // 5% reduction in latency.

		    // - if min-cost route is the direct route, just use it
		    // - if direct-cost route is infinite, then no point
		    //   comparing to the min-cost hop route
		    // - if min-cost route is not much better than direct
		    //   route, use direct route
                    if (minhop == dst.info.id ||
                            directLatency == resetLatency ||
                            min * directBonus < directLatency) {
			// TODO (high priority): can you get a short overflow with above? directBonus is a double
                        rec.via = minhop;
                        recs.add(rec);
                    } else {
                        // At this point,
                        //   min-cost route is not the direct route &&
                        //     src->dst is *not* infinite &&
                        //     min * directBonus >= src->dst
                        // So, recommend the direct route, if that is working.
                        rec.via = dst.info.id;
                        recs.add(rec);
                    }
                }
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
                session.send(ByteBuffer.wrap(buf), new InetSocketAddress(addr, port));
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
            sendObject(o,
                    nid == 0 ? coordNode : (myNid == 0 ?
                        coordNodes.get(nid) : nodes.get(nid).info),
                    nid) : 0;
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
        NodeState src = nodes.get(m.src);
        for (int i = 0; i < m.probeTable.length; i++)
            src.latencies.put(memberNids.get(i), m.probeTable[i]);
        // TODO we aren't setting node.{hop,cameUp,isHopRecommended=false}...
    }

    private void handleRecommendations(RoutingRecs msg) {
        ArrayList<Rec> recs = msg.recs;
        NodeState r = nodes.get(msg.src);
        r.dstsPresent.clear();
        r.remoteFailures.clear();
        for (Rec rec : recs) {
            r.dstsPresent.add(rec.dst);
            if (nodes.get(rec.via).isReachable) {
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
                } else {
                    // blindly trust the recommendations
                    NodeState node = nodes.get(rec.dst);
                    if (node.hop == 0 || node.isDead) {
                        node.cameUp = true;
			node.isDead = false;
		    }
                    node.isHopRecommended = true;
                    node.hop = rec.via;
                }
            }
        }

	if (scheme != RoutingScheme.SQRT_SPECIAL) {
	    /*
	     * get the full set of dsts that we depend on this node for. note
	     * that the set of nodes it's actually serving may be different.
	     */
	    // TODO (low priority): just use dstsPresent instead of remoteFailures
	    for (NodeState dst : nodeDefaultRSs.get(r)) {
		if (!r.dstsPresent.contains(dst.info.id)) {
		    /*
		     * there was a comm failure between this rendezvous and the
		     * dst for which this rendezvous did not provide a
		     * recommendation. this a proximal rendezvous failure, so that if
		     * necessary during the next phase, we will find failovers.
		     */
		    r.remoteFailures.add(dst);
		}
	    }
	}
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
     * Counts the number of paths to a particular node.
     *
     * Note that this does not get run whenever nodes become reachable, only
     * when they become unreachable (and also in batch periodically).
     * Furthermore, we do not run this whenever we get a measurement packet.
     * The reason for these infelicities is one of performance.
     *
     * The logic among hop, isHopRecommended, and cameUp is tricky.
     */
    private int findPaths(NodeState node, boolean batch) {
        ArrayList<NodeState> clients = getAllRendezvousClients();
        ArrayList<NodeState> servers = lastRendezvousServers;
        HashSet<NodeState> options = new HashSet<NodeState>();
        short min = resetLatency;
        short nid = node.info.id;
        boolean wasDead = node.hop == 0;
        NodeState self = nodes.get(myNid);

        // we would like to keep recommended nodes (they should be the best
        // choice, but also we have no ping data). but if it was not obtained
        // via recommendation (i.e., a previous findPaths() got this hop), then
        // we should feel free to update it.
        if (node.hop == 0) {
            node.isHopRecommended = false;
        } else {
            // we are not adding the hop
            if (!node.isHopRecommended) {
                node.hop = 0;
            }
        }

	// Unless node.hop == 0, this code below is useless
	// We would like to measure this...
	//   keep track of subping.

        // direct hop
        if (node.isReachable) {
            options.add(node);
            if (!node.isHopRecommended) {
                node.hop = node.info.id;
                min = self.latencies.get(node.info.id);
            }
        }
	else {
	    // If it is alive, we will set it to false in the next few lines
	    node.isDead = true;
	}

        // find best rendezvous client. (`clients` are all reachable.)
        for (NodeState client : clients) {
            int val = client.latencies.get(nid);
            if (val != resetLatency) {
		if(!node.isReachable)
		    node.isDead = false;

                options.add(client);
                val += self.latencies.get(client.info.id);
                if (!node.isHopRecommended && val < min) {
                    node.hop = client.info.id;
                    min = (short) val;
                }
            }
        }

        // see if a rendezvous server can serve as the hop. (can't just iterate
        // through hopOptions, because that doesn't tell us which server to go
        // through.) using the heuristic of just our latency to the server
        for (NodeState server : servers) {
            if (server.dstsPresent.contains(nid)) {
                options.add(server);
                short val = self.latencies.get(server.info.id);
                if (node.hop == 0 && val < min) {
                    node.hop = server.info.id;
                    min = val;
                }
            }
        }

        boolean isDead = node.hop == 0;
        // seems that (!isDead && wasDead) can be true, if a hop is found here
        // from a measurement (from a rclient).
        boolean cameUp = !isDead && wasDead || node.cameUp;
        boolean wentDown = isDead && !wasDead;
        // reset node.cameUp
        node.cameUp = false;

        // we always print something in non-batch mode. we also print stuff if
        // there was a change in the node's up/down status. if a node is reachable
        // then findPaths(node,) will only be called during batch processing, and
        // so wasDead will have been set either by the last unreachable call or by
        // the previous batch call. thus, the first batch call after a node goes
        // up, the "up" message will be printed.
        if (!batch || cameUp || wentDown) {
            String stateChange = cameUp ? " up" : (wentDown ? " down" : "");
            log("node " + node + stateChange + " hop " + node.hop + " total "
                    + options.size());
        }

        return options.size();
    }

    /**
     * Counts the avg number of one-hop or direct paths available to nodes
     * Calls findPaths(node) for all other nodes. This code is supposed to
     * a) find out a node is alive, and b) find the optimal one-hop route to
     * this destination.
     * @return
     */
    private Pair<Integer, Integer> findPathsForAllNodes() {
        NodeState myState = nodes.get(myNid);
        int count = 0;
        int numNodesReachable = 0;
        for (NodeState node : otherNodes) {
            int d = findPaths(node, true);
            count += d;
            numNodesReachable += d > 0 ? 1 : 0;
        }
        if (numNodesReachable > 0)
            count /= numNodesReachable;
        return Pair.of(numNodesReachable, count);
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
         * down.
         *
         * invariants:
         *  - always refers to a member or 0; enforced in updateMembers()
         *  - never refers to dead node; enforced in resetTimeoutAtNode()
         *  - may be nid (may be dst)
         *  - initially defaults to dst (since we don't know hops to it)
         *  - never refers to the owning neuronnode (never is src)
         *  - cannot be nid if !isReachable
         */
        public short hop;


	/**
	 * Keeps track of whether any node (including ourself) receives measurements
	 * to the destination. Only consider this if node.isReachable is false.
	 */
	public boolean isDead = false;


        /**
         * this is set at certain places where we determine that a node is
         * alive, and reset in the next findPaths().  the only reason we need
         * this is to help produce the correct logging output for the
         * effectiveness timing analysis.
         */
        public boolean cameUp;

        /**
         * this indicates how we got this hop.  this is set in
         * handleRecommendations(), reset in resetTimeoutAtNode(), and
         * read/reset from batch-mode findPaths().  if it was recommended to
         * us, then we will want to keep it; otherwise, it was just something
         * we found in failover mode, so are free to wipe it out.  this var has
         * no meaning when hop == 0.
         */
        public boolean isHopRecommended;

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
         * dstsPresent, the complement of remoteFailures (in defaultClients).
         */
        public final HashSet<Short> dstsPresent = new HashSet<Short>();


        /**
         * this is unused at the moment. still need to re-design.
         */
        public final HashSet<Short> hopOptions = new HashSet<Short>();

        public NodeState(NodeInfo info) {
            this.info = info;
            this.hop = info.id;
            latencies.put(info.id, (short) 0);
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















class NodeInfo  {
	short id;
	int port;
	InetAddress addr;
}
class Rec  {
	short dst;
	short via;
}
class Subprobe  {
	long time;
	InetSocketAddress src;
	InetSocketAddress nod;
	byte type;
}
class PeerPing  {
	long time;
	InetSocketAddress src;
}
class PeerPong  {
	long time;
	InetSocketAddress src;
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
	byte[] inflation;
}

class Serialization {


	public void serialize(Object obj, DataOutputStream out) throws IOException {
		if (false) {}

		else if (obj.getClass() == NodeInfo.class) {
			NodeInfo casted = (NodeInfo) obj; out.writeInt(((int) serVersion) << 8 | 0);
			out.writeShort(casted.id);
			out.writeInt(casted.port);
			{ byte[] buf = casted.addr.getAddress(); out.writeInt(buf.length); out.write(buf); }
		}
		else if (obj.getClass() == Rec.class) {
			Rec casted = (Rec) obj; out.writeInt(((int) serVersion) << 8 | 1);
			out.writeShort(casted.dst);
			out.writeShort(casted.via);
		}
		else if (obj.getClass() == Subprobe.class) {
			Subprobe casted = (Subprobe) obj; out.writeInt(((int) serVersion) << 8 | 2);
			out.writeLong(casted.time);
			{ byte[] buf = casted.src.getAddress().getAddress(); out.writeInt(buf.length); out.write(buf); }
			out.writeInt(casted.src.getPort());
			{ byte[] buf = casted.nod.getAddress().getAddress(); out.writeInt(buf.length); out.write(buf); }
			out.writeInt(casted.nod.getPort());
			out.writeByte(casted.type);
		}
		else if (obj.getClass() == PeerPing.class) {
			PeerPing casted = (PeerPing) obj; out.writeInt(((int) serVersion) << 8 | 3);
			out.writeLong(casted.time);
			{ byte[] buf = casted.src.getAddress().getAddress(); out.writeInt(buf.length); out.write(buf); }
			out.writeInt(casted.src.getPort());
		}
		else if (obj.getClass() == PeerPong.class) {
			PeerPong casted = (PeerPong) obj; out.writeInt(((int) serVersion) << 8 | 4);
			out.writeLong(casted.time);
			{ byte[] buf = casted.src.getAddress().getAddress(); out.writeInt(buf.length); out.write(buf); }
			out.writeInt(casted.src.getPort());
		}
		else if (obj.getClass() == Msg.class) {
			Msg casted = (Msg) obj; out.writeInt(((int) serVersion) << 8 | 5);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		}
		else if (obj.getClass() == Join.class) {
			Join casted = (Join) obj; out.writeInt(((int) serVersion) << 8 | 6);
			{ byte[] buf = casted.addr.getAddress(); out.writeInt(buf.length); out.write(buf); }
			out.writeInt(casted.port);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		}
		else if (obj.getClass() == Init.class) {
			Init casted = (Init) obj; out.writeInt(((int) serVersion) << 8 | 7);
			out.writeShort(casted.id);
			out.writeInt(casted.members.size());
			for (int i = 0; i < casted.members.size(); i++) {
				out.writeShort(casted.members.get(i).id);
				out.writeInt(casted.members.get(i).port);
				{ byte[] buf = casted.members.get(i).addr.getAddress(); out.writeInt(buf.length); out.write(buf); }
			}
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		}
		else if (obj.getClass() == Membership.class) {
			Membership casted = (Membership) obj; out.writeInt(((int) serVersion) << 8 | 8);
			out.writeInt(casted.members.size());
			for (int i = 0; i < casted.members.size(); i++) {
				out.writeShort(casted.members.get(i).id);
				out.writeInt(casted.members.get(i).port);
				{ byte[] buf = casted.members.get(i).addr.getAddress(); out.writeInt(buf.length); out.write(buf); }
			}
			out.writeShort(casted.numNodes);
			out.writeShort(casted.yourId);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		}
		else if (obj.getClass() == RoutingRecs.class) {
			RoutingRecs casted = (RoutingRecs) obj; out.writeInt(((int) serVersion) << 8 | 9);
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
			Ping casted = (Ping) obj; out.writeInt(((int) serVersion) << 8 | 10);
			out.writeLong(casted.time);
			out.writeShort(casted.info.id);
			out.writeInt(casted.info.port);
			{ byte[] buf = casted.info.addr.getAddress(); out.writeInt(buf.length); out.write(buf); }
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		}
		else if (obj.getClass() == Pong.class) {
			Pong casted = (Pong) obj; out.writeInt(((int) serVersion) << 8 | 11);
			out.writeLong(casted.time);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		}
		else if (obj.getClass() == Measurements.class) {
			Measurements casted = (Measurements) obj; out.writeInt(((int) serVersion) << 8 | 12);
			out.writeInt(casted.probeTable.length);
			for (int i = 0; i < casted.probeTable.length; i++) {
				out.writeShort(casted.probeTable[i]);
			}
			out.writeInt(casted.inflation.length); out.write(casted.inflation);
			out.writeShort(casted.src);
			out.writeShort(casted.version);
			out.writeShort(casted.session);
		}
	}

	public static byte serVersion = 2;
	public Object deserialize(DataInputStream in) throws IOException {
		int header = readInt(in);
		if ((header & 0xff00) != ((int) serVersion) << 8) return null;
		int msgtype = header & 0xff;
		switch (msgtype) {

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
		case 2: { // Subprobe
			Subprobe obj;
			{
				obj = new Subprobe();
				{
					obj.time = in.readLong();
				}
				{
					InetAddress addr;
					{
						byte[] buf;
						{

							buf = new byte[readInt(in)];
							in.read(buf);

						}

						addr = InetAddress.getByAddress(buf);

					}
					int port;
					{
						port = readInt(in);
					}

					obj.src = new InetSocketAddress(addr, port);

				}
				{
					InetAddress addr;
					{
						byte[] buf;
						{

							buf = new byte[readInt(in)];
							in.read(buf);

						}

						addr = InetAddress.getByAddress(buf);

					}
					int port;
					{
						port = readInt(in);
					}

					obj.nod = new InetSocketAddress(addr, port);

				}
				{
					obj.type = in.readByte();
				}
			}
			return obj;
		}
		case 3: { // PeerPing
			PeerPing obj;
			{
				obj = new PeerPing();
				{
					obj.time = in.readLong();
				}
				{
					InetAddress addr;
					{
						byte[] buf;
						{

							buf = new byte[readInt(in)];
							in.read(buf);

						}

						addr = InetAddress.getByAddress(buf);

					}
					int port;
					{
						port = readInt(in);
					}

					obj.src = new InetSocketAddress(addr, port);

				}
			}
			return obj;
		}
		case 4: { // PeerPong
			PeerPong obj;
			{
				obj = new PeerPong();
				{
					obj.time = in.readLong();
				}
				{
					InetAddress addr;
					{
						byte[] buf;
						{

							buf = new byte[readInt(in)];
							in.read(buf);

						}

						addr = InetAddress.getByAddress(buf);

					}
					int port;
					{
						port = readInt(in);
					}

					obj.src = new InetSocketAddress(addr, port);

				}
			}
			return obj;
		}
		case 5: { // Msg
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
		case 6: { // Join
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
		case 7: { // Init
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
		case 8: { // Membership
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
		case 9: { // RoutingRecs
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
		case 10: { // Ping
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
		case 11: { // Pong
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
		case 12: { // Measurements
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

		default: throw new RuntimeException("unknown obj type");
		}
	}

	private byte[] readBuffer = new byte[4];

	// read in a big-endian 4-byte integer
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
