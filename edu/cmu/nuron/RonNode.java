package edu.cmu.nuron;

import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import edu.cmu.nuron.msg.InitMsg;
import edu.cmu.nuron.msg.RoutingMsg;

public class RonNode extends Thread {
	int iNodeId;
	boolean bCoordinator;
	String sCoordinatorIp;
	int iCoordinatorPort;
	int numNodes;
	Integer numJoins;
	Integer numJoinsLock;
	
	Integer allTableLock; // TODo :: need to implement fine grained locking
	int[][] probeTable;
	int[] forwardingTable; // contains the ids of the next hop nodes to reach the i's node
	int[] minCostForwardingTable; // contains cost to reach i'th node, using the next hop nodes in the forwarding table.
	
	Semaphore semDone;
	Semaphore semGo;
	
	boolean bGotFirstRoutingUpdate;

	public RonNode(int id, String cName, int cPort, int num_nodes) {
		iNodeId = id;
		sCoordinatorIp = cName;
		iCoordinatorPort = cPort;
		numNodes = num_nodes;
		numJoins = new Integer(0);
		numJoinsLock = new Integer(0);
		probeTable = new int[numNodes][numNodes];
		forwardingTable = new int[numNodes];
		minCostForwardingTable = new int[numNodes];
		allTableLock = new Integer(1);
		semDone = new Semaphore(0);
		semGo = new Semaphore(0);
		
		bGotFirstRoutingUpdate = false;
		
		if (iNodeId == 0) {
			bCoordinator = true;
		} else {
			bCoordinator = false;
		}
	}
	
	public void run() {
		boolean done = false;

		if (!bCoordinator) {
			// start a thread, that listens on port (iCoordinatorPort + iNodeId), to look-out for routing updates
			RoutingUpdateServerThread rust = new RoutingUpdateServerThread(iCoordinatorPort + iNodeId, iNodeId, numNodes, this);
			rust.start();
			//System.out.println(iNodeId + " started RUST at port " + (iCoordinatorPort + iNodeId));
		}

		// if you are a co-oridinator wait for (n-1) other nodes to connect
		if (bCoordinator) {
			int numConnected = 0;
			try {
				ServerSocket ss = new ServerSocket(iCoordinatorPort);
				ss.setReuseAddress(true);
				
				// intitialize probeTable
				for (int i = 0; i < numNodes; i++) {
					// TODO :: need to assign random path weights here.
					probeTable[i][i] = 0;
					for (int j = (i + 1); j < numNodes; j++) {
						probeTable[i][j] = probeTable[j][i] = 5;
					}
				}
				
				// FOR TESTING
				for (int j = 1; j < numNodes; j++) {
					probeTable[0][j] = probeTable[j][0] = 1;
				}

				for (int i = 0; i < numNodes; i++) {
					forwardingTable[i] = iNodeId; // I am the next hop to all others to begin with
					// set correct minCosts
					minCostForwardingTable[i] = probeTable[iNodeId][i];
				}
				//System.out.println("---> " + printForwardingTable());
				
				//System.out.println(iNodeId + " waiting for others to join the party");
				Semaphore semAllClientsConnected = new Semaphore(0);
				// wait for all the nodes to join!
				while(numConnected < (numNodes -1)) {
					Socket incoming = ss.accept();
					numConnected++;
					// co-ordinator assigns node id to the connecting end-point
					ClientHandlerThread worker = new ClientHandlerThread(incoming, this, numNodes, semAllClientsConnected);
					worker.start();
				}
				ss.close();
	            
				// start a thread, that listens on port (iCoordinatorPort + iNodeId), to look-out for routing updates
				RoutingUpdateServerThread rust = new RoutingUpdateServerThread(iCoordinatorPort + iNodeId, iNodeId, numNodes, this);
				rust.start();
				//System.out.println(iNodeId + " started RUST at port " + (iCoordinatorPort + iNodeId));

				// wait for all workers to exit.
	    		do {
    				synchronized(numJoinsLock) {
    					if (numJoins == (numNodes -1)) {
    						done = true;
    						semAllClientsConnected.release(numNodes - 1);
    					}
    				}
	    		} while (!done);
				
				semGo.release();
				//System.out.println(iNodeId + " everyone joined the party");
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
				// System.out.println("Sending join!");
				InetAddress ia = InetAddress.getLocalHost();
				writer.writeBytes("join " + ia.getHostAddress() + " " + iNodeId + "\n");
				//System.out.println("Sent join!");

				// The co-ordinator sent us the initial probe table - read it.
				InitMsg im = null;
				while(im == null) {
					//System.out.println("Trying to read!");
					try {
						im  = (InitMsg) reader.readObject();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				//System.out.println("INCOMING MSG FROM CO-ORD!!! - " + im.toString());
				//iNodeId = im.getId();
				synchronized(allTableLock) {
					im.getProbeTable(probeTable[iNodeId]);
					for (int i = 0; i < numNodes; i++) {
						forwardingTable[i] = iNodeId; // I am the next hop to all others to begin with
						// set correct minCosts
						minCostForwardingTable[i] = probeTable[iNodeId][i];
					}

					//System.out.println("---> " + printForwardingTable());
				}
				
				semGo.release();

				reader.close();
				writer.close();
				s.close();
				
			} catch(Exception e){
				System.out.println("Could not connect or connection was interrupted.");
				e.printStackTrace();
			}
		}
		
		////////////////////////////////////////////////////////////////////////////////
		
		boolean bDone = false;
		double routingBandwidth = 0;
		while(!bDone) {
			// All nodes must have joined the overlay by now
			// Irrespective of whether the node is a co-ordinator or not, exchange adjacency tables.
			for (int i = 0; i < numNodes; i++) {
				if (i != iNodeId) {
					// send node i stuff on port (cPort + i)
					// TODO :: FIX_HACK :: i_ip should be not hardcoded.
					
					try {
						String i_ip = InetAddress.getLocalHost().getHostAddress();
						//System.out.println(iNodeId + " trying to send adjecency table to " + i_ip + ":" + (iCoordinatorPort + i));
						Socket s = new Socket(i_ip, (iCoordinatorPort + i));
						DataOutputStream writer = new DataOutputStream(s.getOutputStream());
						
						RoutingMsg rm = new RoutingMsg(iNodeId, numNodes);
						synchronized(allTableLock) {
							rm.populateProbeTable(probeTable[iNodeId]);
						}
						byte[] b = rm.getBytes(rm);
						writer.write(b, 0, b.length);
	
						routingBandwidth += b.length;
						
						writer.close();
						s.close();
					} catch(Exception e) {
						System.out.println(iNodeId + ":" + (iCoordinatorPort + i));
						e.printStackTrace();
					}
				}
			}
			
			bDone = true;
		}
		
		try {
			semDone.acquire();
		} catch (InterruptedException ie) {
			
		}
		
		String pretty_output = "{" + iNodeId + "} Routing Banwidth Overhead = " + routingBandwidth + " bytes";
		//System.out.println(pretty_output);
	}
	
	public void populateInitProbeTable(InitMsg im) {
		if (im != null) {
			synchronized(allTableLock) {
				im.populateProbeTable(probeTable[im.getId()]);
			}
		}
	}

	// applicable only to the parent - called from the ClientHandlerThread when a node joins an overlay.
	public void doneJoiningOverlay() {
		synchronized(numJoinsLock) {
			numJoins += 1;
			System.out.println(numJoins.intValue() + " nodes joined the overlay!");
		}
	}
	
	public int getCoordinatorPort() {
		return iCoordinatorPort;
	}
	
	public void updateRoutingTable(RoutingMsg rm) {
		synchronized(allTableLock) {
			//System.out.println(printForwardingTable());
			rm.getProbeTable(probeTable[rm.getOriginatorId()]);
			updateForwardingTable(rm);
			//System.out.println(printForwardingTable());
		}
	}

	private void updateForwardingTable(RoutingMsg rm) {
		synchronized(allTableLock) {
			for(int i = 0; i< numNodes; i++) {
				
				if (i != iNodeId) {
					//System.out.println("{" + iNodeId + " " + i + " " + rm.getOriginatorId() + "} : " + minCostForwardingTable[i] + ", " + probeTable[iNodeId][rm.getOriginatorId()] + ", " + rm.getProbeEntry(i));
					// if (cost from me to this guy who sent rm + cost from him to node i) < my current minimum to get to node i, we have a new one-hop route
					if ( minCostForwardingTable[i] > (probeTable[iNodeId][rm.getOriginatorId()] + rm.getProbeEntry(i)) ) {
						// we should hop through this guy
						minCostForwardingTable[i] = probeTable[iNodeId][rm.getOriginatorId()] + rm.getProbeEntry(i);
						forwardingTable[i] = rm.getOriginatorId();
					}
				}
				
			}
		}
	}
	
	public String printForwardingTable() {
		String out = "{" + iNodeId + "} FW [";
		synchronized(allTableLock) {
			for(int i = 0; i < numNodes; i++) {
				out += forwardingTable[i] + ":" + minCostForwardingTable[i];
				if (i != (numNodes-1)) {
					out += "; ";
				}
			}
		}
		out += "]";
		//System.out.println(out);
		return out;
	}
	
	// callback used by RUST
	public void routingThreadQuit() {
		semDone.release();
	}
	
	synchronized public void requestPermissionToProcessRoutingUpdate() {
		if (!bGotFirstRoutingUpdate) {
			try {
				//System.out.println("{" + iNodeId + "} First RM");
				semGo.acquire();
				bGotFirstRoutingUpdate = true;
			} catch (InterruptedException ie) {
				
			}
		}
	}
}
