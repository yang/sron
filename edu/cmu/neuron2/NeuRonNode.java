package edu.cmu.neuron2;

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

public class NeuRonNode extends Thread {
	private final ExecutorService executor;
	private final ScheduledExecutorService scheduler;
	private int iNodeId; // TODO non final
	private final boolean bCoordinator;
	private final String sCoordinatorIp;
	private final int basePort;
	private boolean doQuit;

	private final Hashtable<Integer, NodeInfo> nodes = new Hashtable<Integer, NodeInfo>();

	int[][] probeTable; // probeTable[i] = node members[i]'s probe table. value
	// at index j in row i is the link latency between nodes
	// members[i]->members[j].
	// ArrayList<Integer> bestHopTable; // value at index i, is the best hop
	// value for node at index i in members
	// TODO non final
	private int[][] grid;
	private int numCols, numRows;
	private final HashSet<Integer> neighbors = new HashSet<Integer>();
	private final IoServiceConfig cfg = new DatagramAcceptorConfig();

	private Random generator;

	public NeuRonNode(int id, String cName, int cPort,
			ExecutorService executor, ScheduledExecutorService scheduler) {
		iNodeId = id;
		sCoordinatorIp = cName;
		basePort = cPort;
		coordNode.id = 0;
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
		// bestHopTable = new ArrayList<Integer>();
		grid = null;
		numCols = numRows = 0;
		bCoordinator = iNodeId == 0;
		cfg.getFilterChain().addLast("logger", new LoggingFilter());
		cfg.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));

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
					while (!doQuit) {
						final Socket incoming;
						try {
							incoming = ss.accept();
						} catch (SocketTimeoutException ex) {
							continue;
						}
						final int nodeId = nextNodeId++;
						resetTimeout(nodeId);
						executor.submit(new Runnable() {
							public void run() {
								try {
									Msg.Join msg = (Msg.Join) new ObjectInputStream(
											incoming.getInputStream())
											.readObject();
									try {
										Msg.Init im = new Msg.Init();
										im.id = nodeId;
										synchronized (NeuRonNode.this) {
											addMember(nodeId, msg.addr,
													basePort + nodeId);
											im.members = new ArrayList<NodeInfo>(
													nodes.values());
										}
										new ObjectOutputStream(incoming
												.getOutputStream())
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
						Thread.sleep(1000);
					}
				}

				try {
					// talk to coordinator
					log("sending join");
					Msg.Join msg = new Msg.Join();
					msg.addr = InetAddress.getLocalHost();
					new ObjectOutputStream(s.getOutputStream())
							.writeObject(msg);

					Msg.Init im = (Msg.Init) new ObjectInputStream(s
							.getInputStream()).readObject();
					log("got from coord: " + im);
					assert im.id > 0;
					iNodeId = im.id;
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

			try {
				new DatagramAcceptor().bind(new InetSocketAddress(InetAddress
						.getLocalHost(), basePort + iNodeId), new Receiver(),
						cfg);
				log("server started on " + InetAddress.getLocalHost() + ":"
						+ (basePort + iNodeId));
				scheduler.scheduleAtFixedRate(new Runnable() {
					public void run() {
						synchronized (NeuRonNode.this) {
							pingAll();
						}
					}
				}, 1, 5, TimeUnit.SECONDS);
				scheduler.scheduleAtFixedRate(new Runnable() {
					public void run() {
						synchronized (NeuRonNode.this) {
							broadcastMeasurements();
							broadcastRecommendations();
						}
					}
				}, 1, 10, TimeUnit.SECONDS);
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
		sendObject(ping, 0);
	}

	public final class CoordReceiver extends IoHandlerAdapter {
		@Override
		public void messageReceived(IoSession session, Object obj)
				throws Exception {
			Msg msg = (Msg) obj;
			log("got " + msg.getClass().getSimpleName() + " from " + msg.src);
			synchronized (NeuRonNode.this) {
				resetTimeout(msg.src);
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
				 * that we can no longer contact that node, so we should try to
				 * find something else. Don't change the membership set because
				 * we want everyone's world views to be consistent. Remember to
				 * make it ignore the coordinator.
				 */
				if (msg instanceof Msg.Membership) {
					updateMembers(((Msg.Membership) msg).members);
				} else if (msg instanceof Msg.Measurements) {
					updateNetworkState((Msg.Measurements) msg);
				} else if (msg instanceof Msg.RoutingRecs) {
					// TODO something
				} else if (msg instanceof Msg.Ping) {
					Msg.Ping ping = ((Msg.Ping) msg);
					Msg.Pong pong = new Msg.Pong();
					pong.time = ping.time;
					sendObject(pong, ping.info.id);
				} else if (msg instanceof Msg.Pong) {
					Msg.Pong pong = (Msg.Pong) msg;
					long latency = System.currentTimeMillis() - pong.time;
					log("got pong msg with latency " + latency);
					probeTable[memberNids().indexOf(iNodeId)][memberNids()
							.indexOf(pong.src)] = (int) latency;
				} else {
					throw new Exception("can't handle that message type");
				}
			}
		}
	}

	/**
	 * If we don't hear from a node for this number of seconds, then consider
	 * them dead.
	 */
	private int TIMEOUT = 10;
	private Hashtable<Integer, ScheduledFuture<?>> timeouts = new Hashtable<Integer, ScheduledFuture<?>>();

	private void resetTimeout(final int nid) {
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
		broadcastMembershipChange(nid);
	}

	private void updateMembers(List<NodeInfo> newNodes) {
		nodes.clear();
		for (NodeInfo node : newNodes)
			nodes.put(node.id, node);
		probeTable = new int[nodes.size()][nodes.size()];
		repopulateGrid();
		printGrid();
		printNeighborList();
	}

	/**
	 * TODO XXX OPEN QUESTION HOW TO HANDLE NODE WORLD VIEW INCONSISTENCIES????
	 */
	private void repopulateGrid() {
		numCols = (int) Math.ceil(Math.sqrt(nodes.size()));
		numRows = (int) Math.ceil((double) nodes.size() / (double) numCols);
		grid = new int[numRows][numCols];
		List<Integer> nids = memberNids();
		int m = 0;
		for (int i = 0; i < numRows; i++) {
			for (int j = 0; j < numCols; j++) {
				if (m >= nids.size()) {
					m = 0;
				}
				grid[i][j] = nids.get(m);
				m++;
			}
		}
		repopulateNeighborList();
		repopulateProbeTable();
	}

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
		for (int neighborId : neighbors) {
			s += neighborId + ", ";
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
		for (int src : neighbors) {
			ArrayList<Msg.RoutingRecs.Rec> recs = new ArrayList<Msg.RoutingRecs.Rec>();
			int min = Integer.MAX_VALUE, mini = -1;
			for (int dst : neighbors) {
				for (int i = 0; i < probeTable[src].length; i++) {
					int cur = probeTable[src][i] + probeTable[dst][i];
					if (cur < min) {
						min = cur;
						mini = i;
					}
				}
				recs.add(new Msg.RoutingRecs.Rec(dst, mini));
			}
			Msg.RoutingRecs msg = new Msg.RoutingRecs();
			msg.recs = recs;
			sendObject(msg, src);
		}
	}

	private NodeInfo coordNode = new NodeInfo();

	private void sendObject(final Msg o, int nid) {
		NodeInfo node = nid == 0 ? coordNode : nodes.get(nid);
		log("sending " + o.getClass().getSimpleName() + " to " + nid
				+ " living at " + node.addr + ":" + node.port);
		o.src = iNodeId;
		new DatagramConnector().connect(new InetSocketAddress(node.addr,
				node.port), new IoHandlerAdapter() {
			@Override
			public void sessionCreated(IoSession session) {
				session.write(o);
			}
		}, cfg);
	}

	private synchronized void broadcastMeasurements() {
		Msg.Measurements rm = new Msg.Measurements();
		rm.membershipList = memberNids();
		rm.probeTable = probeTable[rm.membershipList.indexOf(iNodeId)].clone();
		for (int nid : neighbors) {
			sendObject(rm, nid);
		}
	}

	private synchronized void updateNetworkState(Msg.Measurements m) {
		int offset = memberNids().indexOf(m.src);
		// Make sure that we have the exact same world-views before proceeding,
		// as otherwise the neighbor sets may be completely different. Steps can
		// be taken to tolerate differences and to give best-recommendations
		// based on incomplete info, but it may be better to take a step back
		// and re-evaluate our approach to consistency as a whole first. For
		// now, this simple approach will at least work.
		if (offset != -1 && m.membershipList.equals(memberNids())) {
			for (int i = 0; i < m.probeTable.length; i++) {
				probeTable[offset][i] = m.probeTable[i];
			}
		}
	}

	public void quit() {
		this.doQuit = true;
	}
}
