package edu.cmu.neuron2;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Semaphore;

import edu.cmu.neuron2.msg.InitMsg;
import edu.cmu.neuron2.msg.MembershipMsg;
import edu.cmu.neuron2.msg.RoutingMsg;

public class NeuRonNode extends Thread implements IRonNode {
	int iNodeId;
	boolean bCoordinator;
	String sCoordinatorIp;
	int iCoordinatorPort;

	boolean done;
	
	MembershipUpdateServerThread must; 	
	RoutingUpdateServerThread rust;
	RoutingUpdateThread rut;
	//NextHopRecoThread nhrt;
	//NextHopRecoServerThread nhrt_s;

	Semaphore semStateLock;
	
	ArrayList<Integer> members;
	ArrayList<Integer> probeTable;   // value at index i, is the link latency to node at index i in members
	ArrayList<Integer> bestHopTable; // value at index i, is the best hop value for node at index i in members
	int[][] grid;
	int numCols, numRows;
	ArrayList<Integer> neighbors;

    Random generator;
	
	public NeuRonNode(int id, String cName, int cPort) {
		iNodeId = id;
		sCoordinatorIp = cName;
		iCoordinatorPort = cPort;
		
		done = false;
		
		must = null;
		rust = null;
		rut = null;
		//nhrt = null;
		//nhrt_s = null;
		
		generator = new Random(iNodeId);
		
		semStateLock = new Semaphore(1);

		members = new ArrayList<Integer>();
		probeTable = new ArrayList<Integer>();
		bestHopTable = new ArrayList<Integer>();
		grid = null;
		numCols = numRows = 0;
		neighbors = new ArrayList<Integer>();
				
		if (iNodeId == 0) {
			bCoordinator = true;
		} else {
			bCoordinator = false;
		}
	}
	
	public void run() {
		if (bCoordinator) {
			int numConnected = 0;
			try {
				ServerSocket ss = new ServerSocket(iCoordinatorPort);
				ss.setReuseAddress(true);

				System.out.println("Beep!");
				// wait for nodes to join!
				while(!done) {
					Socket incoming = ss.accept();
					numConnected++;
					// co-ordinator assigns node id to the connecting end-point
					ClientHandlerThread worker = new ClientHandlerThread(incoming, this, numConnected);
					worker.start();
				}
				ss.close();
				System.out.println("Bebop!");

			} catch(Exception e) {
				System.out.println("Error: Could not bind to port, or a connection was interrupted.");
			}
		} 
		
		// if you are not a co-ordinator
		// a. join the overlay by contacting the co-ordinator
		// b. start a server thread to listen to routing updates
		// c. send routing updates to everyone else.
		else {
			try {

				// System.out.println("blah");

				// make sure our man the co-ordinator has started his server socket
				try {
					//System.out.println(iNodeId + " sleeping ...");
					Thread.sleep(5000);
					//System.out.println(iNodeId + " woke-up ...");
				} catch (InterruptedException ie) {
		
				}

				// Connect to the co-ordinator
				Socket s = new Socket(sCoordinatorIp, iCoordinatorPort);
				ObjectInputStream reader = new ObjectInputStream(s.getInputStream());
				DataOutputStream writer = new DataOutputStream(s.getOutputStream());

				// Send Join request to the Co-ordinator
				System.out.println("Sending join!");
				InetAddress ia = InetAddress.getLocalHost();
				writer.writeBytes("join " + ia.getHostAddress() + " " + iNodeId + "\n");
				//System.out.println("Sent join!");

				InitMsg im = null;
				while(im == null) {
					//System.out.println("Trying to read!");
					try {
						im  = (InitMsg) reader.readObject();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				System.out.println("INCOMING MSG FROM CO-ORD!!! - " + im.toString());
				iNodeId = im.getId();
				handleMembershipChange(im);
				
				// start a thread, that listens on port (iCoordinatorPort + iNodeId), to look-out for routing updates
				rust = new RoutingUpdateServerThread(iCoordinatorPort + iNodeId, iNodeId, this);
				rust.start();

				// start a thread, that listens on port ((iCoordinatorPort + 1000) + iNodeId), to look-out for routing updates
				must = new MembershipUpdateServerThread((iCoordinatorPort + 1000) + iNodeId, iNodeId, this);
				must.start();

				// start a thread, that loops and send routing updates periodically
				rut = new RoutingUpdateThread(iNodeId, this);
				rut.start();

				writer.writeBytes("done " + iNodeId + "\n");

				reader.close();
				writer.close();
				s.close();
				
			} catch(Exception e){
				System.out.println("Could not connect or connection was interrupted.");
				e.printStackTrace();
			}
		}
	}
	
	
	public void aquireStateLock() {
		semStateLock.acquireUninterruptibly();
	}

	public void releaseStateLock() {
		semStateLock.release();
	}

	public void addMemberNode(int node_id) {
		if (members != null) {
			synchronized(members) {
				boolean bFlag = false;
				for (Integer nid: members) {
					if (nid == node_id) {
						bFlag = true;
						break;
					}
				}
				if (bFlag == false) {
					members.add(new Integer(node_id));
					broadcastMembershipChange(node_id);
				}
			}
		}
	}
	
	public void broadcastMembershipChange() {
		if (members != null) {
			synchronized(members) {
				
				for (Integer memberId: members) {
					// XXX: we are using the co-ords ip (but that's ok, because this is run on a single machine)
					try {
						Socket s = new Socket(sCoordinatorIp, (iCoordinatorPort + 1000) + memberId);
						BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
						ObjectOutputStream writer = new ObjectOutputStream(s.getOutputStream());
						MembershipMsg mm = new MembershipMsg(iNodeId, members);
						writer.writeObject(mm);
						reader.close();
						writer.close();
						s.close();
					} catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		}		
	}

	public void broadcastMembershipChange(int exceptionNodeId) {
		if (members != null) {
			synchronized(members) {
				
				for (Integer memberId: members) {
					if (memberId != exceptionNodeId) {
						// XXX: we are using the co-ords ip (but that's ok, because this is run on a single machine)
						try {
							System.out.println("sending to port: " + ((iCoordinatorPort + 1000) + memberId));

					        DatagramSocket s = new DatagramSocket();
							MembershipMsg mm = new MembershipMsg(iNodeId, members);
					        byte[] buf = MembershipMsg.getBytes(mm);
					        InetAddress sAddr = InetAddress.getByName(sCoordinatorIp);
					        DatagramPacket packet = new DatagramPacket(buf, buf.length, sAddr, ((iCoordinatorPort + 1000) + memberId));
					        s.send(packet);
							s.close();
						} catch(Exception e){
							e.printStackTrace();
						}
					}
				}
			}
		}		
	}

	
	public void populateMemberList(InitMsg im) {
		if (im != null) {
			synchronized(members) {
				im.initMemberList(members);
			}
		}
	}
	
	public void readInMemberList(InitMsg im) {
		if (im != null) {
			synchronized(members) {
				im.getMemberList(members);
			}
		}
	}
	
	public void handleMembershipChange(MembershipMsg mm) {
		if (mm != null) {
				aquireStateLock();
				for (Iterator it = members.iterator (); it.hasNext (); ) {
					it.next();
					it.remove(); // NOTE :: this is it.remove and not members.remove (which would result in a ConcurrentModificationException!)
				}
				mm.getMemberList(members);
				printMembership();
				repopulateGrid();
				printGrid();
				printNeighborList();
				releaseStateLock();
		}
	}

	public void handleMembershipChange(InitMsg im) {
		if (im != null) {
			aquireStateLock();
			im.getMemberList(members);
			//printMembership();
			repopulateGrid();
			printGrid();
			printNeighborList();
			releaseStateLock();
		}
	}

	// NOTE :: assumes that the state is locked already
	private void repopulateGrid() {
		numCols = (int) Math.ceil(Math.sqrt(members.size()));
		numRows = (int) Math.ceil((double)members.size() / (double)numCols);
		
		grid = new int[numRows][numCols];
		
		//System.out.println("Cols = " + numCols + "; Rows = " + numRows + "; Members = " + members.size());
		int m = 0;
		for (int i = 0; i < numRows; i++) {
			for (int j = 0; j < numCols; j++) {
				if (m >= members.size()) {
					m = 0;
				}
				//System.out.println("i = " + i + "; j = " + j + "; m = " + m + "; member = " + members.get(m));
				grid[i][j] = members.get(m);
				m++;
			}
		}
		
		repopulateNeighborList();
		repopulateProbeTable();
	}
	
	// NOTE :: assumes that the state is locked already
	private void repopulateNeighborList() {
		if (neighbors != null) {
			for (Iterator it = neighbors.iterator (); it.hasNext (); ) {
				it.next();
				it.remove(); // NOTE :: this is it.remove and not members.remove (which would result in a ConcurrentModificationException!)
			}
		}
		for (int i = 0; i < numRows; i++) {
			for (int j = 0; j < numCols; j++) {
				if (grid[i][j] == iNodeId) {
					// all the nodes in row i, and all the nodes in column j are belong to us :)
					//System.out.println("Node " + iNodeId + " looking for new neighbors. i = " + i + "; j = " + j);
					for (int x = 0; x < numCols; x++) {
						if (grid[i][x] != iNodeId) {
							addNeighborNode(grid[i][x]);
						}
					}
					for (int x = 0; x < numRows; x++) {
						if (grid[x][j] != iNodeId) {
							addNeighborNode(grid[x][j]);
						}
					}
				}
			}
		}
	}

	// NOTE :: assumes that the state is locked already
	private void repopulateProbeTable() {
		if (probeTable != null) {
			for (Iterator it = probeTable.iterator (); it.hasNext (); ) {
				it.next();
				it.remove(); // NOTE :: this is it.remove and not members.remove (which would result in a ConcurrentModificationException!)
			}
		}
		for (Integer memberId: members) {
			if (memberId == iNodeId) {
				probeTable.add(0);
			} else {
				probeTable.add(generator.nextInt());
			}
		}
	}

	// NOTE :: assumes that the state is locked already
	private void addNeighborNode(int node_id) {
		synchronized(neighbors) {
			boolean bFlag = false;
			for (Integer nid: neighbors) {
				if (nid == node_id) {
					bFlag = true;
					break;
				}
			}
			if (bFlag == false) {
				neighbors.add(new Integer(node_id));
			}
		}
	}

	public int getCoordinatorPort() {
		return iCoordinatorPort;
	}
	
	public void printMembership() {
		String s = new String("Membership for Node " + iNodeId + ". Membership = [");
		synchronized (members) {
			for (Integer memberId: members) {
				s += memberId + ", ";
			}
			s += "]";
		}
		System.out.println(s);
	}

	public void printNeighborList() {
		String s = new String("Neighbors for Node " + iNodeId + ". Neighbors = [");
		synchronized (neighbors) {
			for (Integer neighborId: neighbors) {
				s += neighborId + ", ";
			}
			s += "]";
		}
		System.out.println(s);
	}

	public void printGrid() {
		String s = new String("Grid for Node " + iNodeId + ".\n");
		if (grid != null) {
			for (int i = 0; i < numRows; i++) {
				for (int j = 0; j < numCols; j++) {
					s += "\t " + grid[i][j];
				}
				s += "\n";
			}
		}
		System.out.println(s);
	}
	
	
	public void sendAllNeighborsAdjacencyTable() {
		aquireStateLock();
		RoutingMsg rm = new RoutingMsg(iNodeId, members, probeTable);

		try {
			byte[] buf = RoutingMsg.getBytes(rm);
			
			for (Integer neighborId: neighbors) {
				try {
					//System.out.println("Node " + iNodeId + ": sending to port " + (iCoordinatorPort + neighborId));
	
			        DatagramSocket s = new DatagramSocket();
			        InetAddress sAddr = InetAddress.getByName(sCoordinatorIp);
			        DatagramPacket packet = new DatagramPacket(buf, buf.length, sAddr, iCoordinatorPort + neighborId);
			        s.send(packet);
					s.close();
				} catch(Exception e){
					e.printStackTrace();
				}
			}
		} catch(Exception e){
			e.printStackTrace();
		}
		
		releaseStateLock();
	}

	public void quit() {
		done = true;
		
		if (must != null) {
			must.quit();
		}
		
		if (rust != null) {
			rust.quit();
		}
		
		if (rut != null) {
			rut.quit();
		}

//		if (nhrt != null) {
//			nhrt.quit();
//		}
//
//		if (nhrt_s != null) {
//			nhrt_s.quit();
//		}

	}
}
