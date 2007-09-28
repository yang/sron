package edu.cmu.neuron2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoServiceConfig;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.LoggingFilter;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.nio.DatagramAcceptor;
import org.apache.mina.transport.socket.nio.DatagramAcceptorConfig;
import org.apache.mina.transport.socket.nio.DatagramConnector;

import edu.cmu.neuron2.Msg.RoutingRecs.Rec;

public class NeuRonNode extends Thread {
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private int iNodeId; // TODO non final
    private final boolean bCoordinator;
    private final String sCoordinatorIp;
    private final int basePort;
    private boolean doQuit;

    private final Hashtable<Integer, NodeInfo> nodes = new Hashtable<Integer, NodeInfo>();

    // probeTable[i] = node members[i]'s probe table. value
    // at index j in row i is the link latency between nodes members[i]->members[j].
    long[][] probeTable;
    private GridNode[][] grid;
    private int numCols, numRows;
    private final HashSet<GridNode> overflowNeighbors = new HashSet<GridNode>();
    private Hashtable<Integer, Integer> nextHopTable = new Hashtable<Integer, Integer>();
    private final IoServiceConfig cfg = new DatagramAcceptorConfig();

    private Random generator;
    private int currentStateVersion;

    public static final int ADJ_AND_RECO_INTERVAL_IN_SEC = 10;
    public static final int PING_INTERVAL_IN_SEC = 5;

    // in bytes per second
    private double routingBandwidth, routingOverheadInBytes;
    private long startTime, endTime;

    private final ByteArrayOutputStream baos;
    private final ObjectOutputStream oos;

    public NeuRonNode(int id, String cName, int cPort, ExecutorService executor, ScheduledExecutorService scheduler) {
        iNodeId = id;
        sCoordinatorIp = cName;
        basePort = cPort;
        coordNode.id = 0;
        currentStateVersion = 0;
        try {
            coordNode.addr = InetAddress.getByName(sCoordinatorIp);
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
        coordNode.port = basePort;
        this.executor = executor;
        this.scheduler = scheduler;
        generator = new Random(iNodeId);
        probeTable = null;
        grid = null;
        numCols = numRows = 0;
        bCoordinator = iNodeId == 0;
        //cfg.getFilterChain().addLast("logger", new LoggingFilter());
        cfg.getFilterChain().addLast("codec",
                new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));

        routingBandwidth = routingOverheadInBytes =0;
        startTime = System.currentTimeMillis();
        baos = new ByteArrayOutputStream();
        try {
            oos = new ObjectOutputStream(baos);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private void log(String msg) {
        System.out.println("node " + iNodeId + ":\n  " + msg);
    }

    private void err(String msg) {
        System.out.println("node " + iNodeId + ":\n  " + msg);
    }

    public void run() {
        if (bCoordinator) {
            try {
                int nextNodeId = 1;
                Thread.sleep(2000);
                new DatagramAcceptor().bind(new InetSocketAddress(InetAddress.getLocalHost(), basePort),
                                            new CoordReceiver(), cfg);
                ServerSocket ss = new ServerSocket(basePort);
                try {
                    // TODO the coord should also be kept aware of who's alive
                    // and who's not. this means we need to ping the coord, and
                    // the coord needs to maintain timeouts like everyone else.
                    ss.setReuseAddress(true);
                    ss.setSoTimeout(1000);
                    log("Beep!");
                    while (!doQuit) {
                        final Socket incoming;
                        try {
                            incoming = ss.accept();
                        } catch (SocketTimeoutException ex) {
                            continue;
                        }
                        final int nodeId = nextNodeId++;
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
                        s = new Socket(sCoordinatorIp, basePort);
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
                    log("sending join");
                    Msg.Join msg = new Msg.Join();
                    msg.addr = InetAddress.getLocalHost();
                    new ObjectOutputStream(s.getOutputStream())
                            .writeObject(msg);

                    Msg.Init im = (Msg.Init) new ObjectInputStream(s.getInputStream()).readObject();
                    assert im.id > 0;
                    iNodeId = im.id;
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


            startTime = System.currentTimeMillis();
            // now start accepting pings and other msgs,
            // also start sending probes and sending out other msgs
            try {
                new DatagramAcceptor().bind(new InetSocketAddress(InetAddress.getLocalHost(), basePort + iNodeId),
                                            new Receiver(), cfg);
                log("server started on " + InetAddress.getLocalHost() + ":" + (basePort + iNodeId));
                scheduler.scheduleAtFixedRate(new Runnable() {
                    public void run() {
                        synchronized (NeuRonNode.this) {
                            pingAll();
                        }
                    }
                }, 1, PING_INTERVAL_IN_SEC, TimeUnit.SECONDS);
                scheduler.scheduleAtFixedRate(new Runnable() {
                    public void run() {
                        synchronized (NeuRonNode.this) {
                            broadcastMeasurements();
                            broadcastRecommendations();
                        }
                    }
                }, 1, ADJ_AND_RECO_INTERVAL_IN_SEC, TimeUnit.SECONDS);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void pingAll() {
        Msg.Ping ping = new Msg.Ping();
        ping.time = System.currentTimeMillis();
        ping.info = nodes.get(iNodeId);
        for (int nid : nodes.keySet())
            if (nid != iNodeId)
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

    /**
     * coordinator's msg handling loop
     */
    public final class CoordReceiver extends IoHandlerAdapter {
        @Override
        public void messageReceived(IoSession session, Object obj)
                throws Exception {
            Msg msg = (Msg) obj;
            log("got " + msg.getClass().getSimpleName() + " from " + msg.src);
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
            Msg msg = (Msg) obj;
            log("got " + msg.getClass().getSimpleName() + " from " + msg.src);
            synchronized (NeuRonNode.this) {
                /*
                 * TODO Add something similar to resetTimeout here, but rather
                 * than have it change the membership set, just have it note
                 * that we can no longer contact that node, so we may want to
                 * find another rendez-vous. Don't change the membership set
                 * because we want everyone's world views to be consistent.
                 * Remember to make it ignore the coordinator.
                 */
                if (msg.version > currentStateVersion) {
                    if (msg instanceof Msg.Membership) {
                        currentStateVersion = msg.version;
                        updateMembers(((Msg.Membership) msg).members);
                    } else {
                        // i am out of date - request latest membership
                        sendObject(new Msg.MemberPoll(), 0);
                    }
                }
                else if (msg.version == currentStateVersion) {
                    // all msgs go though the same processing loop
                    //   (but we can live that that for now)
                    if (msg instanceof Msg.Membership) {
                        updateMembers(((Msg.Membership) msg).members);
                    } else if (msg instanceof Msg.Measurements) {
                        updateNetworkState((Msg.Measurements) msg);
                    } else if (msg instanceof Msg.RoutingRecs) {
                        handleRecommendation(((Msg.RoutingRecs) msg).recs);
                    } else if (msg instanceof Msg.Ping) {
                        Msg.Ping ping = ((Msg.Ping) msg);
                        Msg.Pong pong = new Msg.Pong();
                        pong.ping_time = ping.time;
                        pong.pong_time = System.currentTimeMillis();
                        sendObject(pong, ping.info.id);
                    } else if (msg instanceof Msg.Pong) {
                        Msg.Pong pong = (Msg.Pong) msg;
                        long currTime = System.currentTimeMillis();

                        resetTimeoutAtNode(pong.src);
                        long rtt = currTime - pong.ping_time;
                        // will not use these due to clock synch problems
                        long latency = pong.pong_time - pong.ping_time;
                        long latency1 = currTime - pong.pong_time;
                        log("got pong msg. one way latency to " + pong.src + " = " + rtt/2);
                        ArrayList<Integer> sortedNids = memberNids();
                        probeTable[sortedNids.indexOf(iNodeId)][sortedNids.indexOf(pong.src)]
                                                                = (int) rtt/2;
                    } else if (msg instanceof Msg.PeeringRequest) {
                        Msg.PeeringRequest pr = (Msg.PeeringRequest) msg;
                        GridNode newNeighbor = new GridNode();
                        newNeighbor.id = pr.src;
                        newNeighbor.bAlive = true;
                        overflowNeighbors.add(newNeighbor);
                    } else {
                        throw new Exception("can't handle that message type");
                    }
                }
                else {
                    err("stale msg from " + msg.src);
                }
            }
        }
    }

    /**
     * If we don't hear from a node for this number of seconds, then consider
     * them dead.
     */
    private int TIMEOUT = PING_INTERVAL_IN_SEC * 5;
    private Hashtable<Integer, ScheduledFuture<?>> timeouts = new Hashtable<Integer, ScheduledFuture<?>>();

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
            }, TIMEOUT, TimeUnit.SECONDS);
            timeouts.put(nid, future);
        }
    }

    private void resetTimeoutAtNode(final int nid) {
        if (nodes.contains(nid)) {
            ScheduledFuture<?> oldFuture = timeouts.get(nid);
            if (oldFuture != null) {
                oldFuture.cancel(false);
            }
            ScheduledFuture<?> future = scheduler.schedule(new Runnable() {
                public void run() {
                    synchronized (NeuRonNode.this) {
                        // O(n)
                        for (int i = 0; i < numRows; i++) {
                            for (int j = 0; j < numCols; j++) {
                                if (grid[i][j].id == nid) {
                                    grid[i][j].bAlive = false;
                                }
                            }
                        }
                    }
                }
            }, TIMEOUT, TimeUnit.SECONDS);
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
        nodes.clear();

        for (NodeInfo node : newNodes) {
            nodes.put(node.id, node);
        }

        Hashtable<Integer, Integer> newNextHopTable = new Hashtable<Integer, Integer>(nodes.size());
        for (NodeInfo node : newNodes) {
            if (node.id != iNodeId) {
                Integer nextHop = nextHopTable.get(node.id);
                if (nextHop == null) {
                    // new node !
                    newNextHopTable.put(node.id, iNodeId);
                }
                else {
                    // check if this old next hop is in the new membership list
                    if (nodes.get(nextHop) != null) {
                        // we have some next hop that is alive - leave it as is
                        newNextHopTable.put(node.id, nextHop);
                    }
                    else {
                        // the next hop vanaished. i am next hop to this node now
                        newNextHopTable.put(node.id, iNodeId);
                    }
                }
            }
            else {
                newNextHopTable.put(iNodeId, iNodeId);
            }
        }
        nextHopTable = newNextHopTable; // forget about the old one


        // TODO :: we lose previous measurements - need to fix this
        // repopulateProbeTable() currently fills in random values
        probeTable = new long[nodes.size()][nodes.size()];
        repopulateGrid();
        printGrid();
        printNeighborList();
    }

    /**
     * TODO XXX OPEN QUESTION HOW TO HANDLE NODE WORLD VIEW INCONSISTENCIES????
     */
    private synchronized void repopulateGrid() {
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
                gn.bAlive = true;
                grid[i][j] = gn;
                m++;
            }
        }
        overflowNeighbors.clear();
        // repopulateNeighborList();
        repopulateProbeTable();
    }

    private synchronized HashSet<GridNode> getNeighborList() {
        HashSet<GridNode> neighborSet = new HashSet<GridNode>();
        for (int r = 0; r < numRows; r++) {
            for (int c = 0; c < numCols; c++) {

                // this can happen at most twice
                if (grid[r][c].id == iNodeId) {
                    // all the nodes in row i, and all the nodes in column j are
                    // belong to us :)

                    // O(N^1.5)   :(
                    for (int x = 0; x < numCols; x++) {
                        if (grid[r][x].id != iNodeId) {
                            GridNode neighbor = grid[r][x];
                            if (neighbor.bAlive) {
                                neighborSet.add(neighbor);
                            }
                            else {
                                log("R node failover!");
                                for (int i = 0; i < numRows; i++) {
                                    if ( (i != r) && (grid[i][c].bAlive == false) ) {
                                        /* (r, x) and (i, c) can't be reached
                                         * (i, x) needs a failover R node
                                         */
                                        for (int j = 0; j < numCols; j++) {
                                            if ( (grid[i][j].id != iNodeId) && (grid[i][j].bAlive == true) ) {
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
                        if (grid[x][c].id != iNodeId) {
                            neighborSet.add(grid[x][c]);
                        }
                    }
                }

            }
        }
        return neighborSet;
    }

    /*
    private void repopulateNeighborList() {
        neighbors.clear();
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                if (grid[i][j] == iNodeId) {
                    // all the nodes in row i, and all the nodes in column j are
                    // belong to us :)
                    for (int x = 0; x < numCols; x++) {
                        if (grid[i][x] != iNodeId) {
                            neighbors.add(grid[i][x]);
                        }
                    }
                    for (int x = 0; x < numRows; x++) {
                        if (grid[x][j] != iNodeId) {
                            neighbors.add(grid[x][j]);
                        }
                    }
                }
            }
        }
    }
    */

    private void repopulateProbeTable() {
        int nodeIndex = memberNids().indexOf(iNodeId);
        for (int i = 0; i < memberNids().size(); i++) {
            if (i == nodeIndex) {
                probeTable[i][i] = 0;
            } else {
                probeTable[nodeIndex][i] = generator.nextInt();
            }
        }

        // for testing
        if (nodeIndex == 0) {
            for (int i = 1; i < memberNids().size(); i++) {
                probeTable[nodeIndex][i] = 1;
            }
        } else {
            probeTable[nodeIndex][0] = 1;
        }
    }

    private void printMembership() {
        String s = new String("Membership for Node " + iNodeId
                + ". Membership = [");
        for (Integer memberId : memberNids()) {
            s += memberId + ", ";
        }
        s += "]";
        log(s);
    }

    private void printNeighborList() {
        String s = new String("Neighbors for Node " + iNodeId
                + ". Neighbors = [");
        HashSet<GridNode> nl = getNeighborList();
        // YYY for (int neighborId : neighbors) {
        for (GridNode neighbor : nl) {
            s += neighbor.id + ", ";
        }
        s += "]";
        log(s);
    }

    private void printGrid() {
        String s = new String("Grid for Node " + iNodeId + ".\n");
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
    private synchronized void broadcastRecommendations() {
        HashSet<GridNode> nl = getNeighborList();
        nl.addAll(overflowNeighbors);
        overflowNeighbors.clear();
        for (GridNode src : nl) {
            ArrayList<Msg.RoutingRecs.Rec> recs = new ArrayList<Msg.RoutingRecs.Rec>();
            long min = Long.MAX_VALUE;
            int mini = -1;
            for (GridNode dst : nl) {
                if (src.id != dst.id) {
                    for (int i = 0; i < probeTable[src.id].length; i++) {
                        // we assume bi-directional links for the time being
                        // i.e. link from a-> b is the same as b -> a
                        long cur = probeTable[src.id][i] + probeTable[dst.id][i];
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
            recordOverhead(msg);
            sendObject(msg, src.id);
        }
    }

    private NodeInfo coordNode = new NodeInfo();

    private void sendObject(final Msg o, int nid) {
        if (nid != iNodeId) {
            NodeInfo node = nid == 0 ? coordNode : nodes.get(nid);
            log("sending " + o.getClass().getSimpleName() + " to " + nid
                    + " living at " + node.addr + ":" + node.port);
            o.src = iNodeId;
            o.version = currentStateVersion;
            log("currVersion = " + currentStateVersion);
            new DatagramConnector().connect(new InetSocketAddress(node.addr, node.port),
                                            new IoHandlerAdapter() {
                @Override
                public void sessionCreated(IoSession session) {
                    session.write(o); // TODO :: need custom serialization
                }
            }, cfg);
        }
    }

    private synchronized void broadcastMeasurements() {
        Msg.Measurements rm = new Msg.Measurements();
        rm.membershipList = memberNids();
        rm.probeTable = probeTable[rm.membershipList.indexOf(iNodeId)].clone();
        HashSet<GridNode> nl = getNeighborList();
        for (GridNode neighbor : nl) {
            recordOverhead(rm);
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

    private synchronized void handleRecommendation(ArrayList<Rec> recs) {
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

    private void recordOverhead(Msg m) {
        try {
            // what a retarded way to record size - need custom serialization!
            oos.writeObject(m);
            synchronized (this) {
                routingOverheadInBytes = baos.size();
            }
            baos.reset();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    public void quit() {
        this.doQuit = true;
        synchronized (NeuRonNode.this) {
            endTime = System.currentTimeMillis();
            long deltaInSec = (endTime - startTime) * 1000;
            routingBandwidth = routingOverheadInBytes / deltaInSec;
        }
        log("Routing Bandwidth = " + routingBandwidth);
    }

}

class GridNode {
    public int id;
    public boolean bAlive;

    public String toString() {
        String s = new String("");
        s += id;
        if (bAlive) {
            s += "(l_up)";
        } else {
            s+= "(l_down)";
        }
        return s;
    }

    public int hashCode()
    {
        return id;
    }

    public boolean equals(Object other)
    {
        if (other != null && getClass() ==
                       other.getClass())
       {  Item otherItem = (Item)other;
          return description.equals(
                    otherItem.description)
              && partNumber ==
                   otherItem.partNumber;
       }
       else
          return false;
    }
}
