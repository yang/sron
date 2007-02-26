package edu.cmu.nuron;

import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Semaphore;

import edu.cmu.nuron.msg.HitsGraphBestHopMsg;
import edu.cmu.nuron.msg.InitMsg;
import edu.cmu.nuron.msg.RoutingMsg;

public class NeuRonNode extends Thread implements IRonNode {
	int iNodeId;
	boolean bCoordinator;
	String sCoordinatorIp;
	int iCoordinatorPort;
	int numNodes;
	ArrayList<Integer> neighbors;
	int numNeighbors;
	Integer numJoins;
	Integer numJoinsLock;
	
	Integer allTableLock; // TODo :: need to implement fine grained locking
	int[][] probeTable;
	int[] forwardingTable; // contains the ids of the next hop nodes to reach the i's node
	int[] minCostForwardingTable; // contains cost to reach i'th node, using the next hop nodes in the forwarding table.

	// table for neighbors (numNeighbors*numNeighbors large)
	int[][] minCostNextHopRecommendation;
	int[][] nextHopMinCost;
	
	Semaphore semDone;	// semaphore used to signal that RUST thread has quit
	Semaphore semGo;	// semaphore used to signal that the intial table sent from the co-ordinator has been received.
	Semaphore semAllJoined;
	
	boolean bGotFirstRoutingUpdate;

	public NeuRonNode(int id, String cName, int cPort, int num_nodes) {
		iNodeId = id;
		sCoordinatorIp = cName;
		iCoordinatorPort = cPort;
		numNodes = num_nodes;
		
		int root = ((int) Math.sqrt(numNodes));
		neighbors = new ArrayList<Integer>(root * 2);

		//String pot = new String(); //pretty output
		
		//column
		for (int i = 0; i < root; i++) {
			int nid = (i * root) + (iNodeId%root);
			if (iNodeId != nid) neighbors.add(nid);
			//pot += ":" + nid;
		}
		//row
		for (int i = 0; i < root; i++) {
			int nid = ( ((iNodeId/root)*root) + i);
			if (iNodeId != nid) neighbors.add(nid);
			//pot += ":" + nid;
		}
		
		//System.out.println("{" + iNodeId + "} " + pot);
		
		numNeighbors = neighbors.size();

		// the index id here does not correspond to the nodeId of the neighbor, 
		// but can be obtained by the value at the corresponding index id in the list "neighbours"
		minCostNextHopRecommendation = new int[numNeighbors][numNeighbors];
		nextHopMinCost = new int[numNeighbors][numNeighbors];
		
		for (int n = 0; n < numNeighbors; n++) {
			int neighbor = neighbors.get(n);
			for (int i = 0; i < numNeighbors; i++) {
				minCostNextHopRecommendation[n][i] = neighbor; // hey neighbor, use yourself as the next hop to get to my neighbors
				nextHopMinCost[n][i] = -1;
			}
		}
		
		numJoins = new Integer(0);
		numJoinsLock = new Integer(0);
		probeTable = new int[numNodes][numNodes];
		forwardingTable = new int[numNodes];
		minCostForwardingTable = new int[numNodes];
		allTableLock = new Integer(1);
		semDone = new Semaphore(0);
		semGo = new Semaphore(0);
		semAllJoined = new Semaphore(0);
		
		bGotFirstRoutingUpdate = false;
		
		if (iNodeId == 0) {
			bCoordinator = true;
		} else {
			bCoordinator = false;
		}
		
		//debug
//		String neigh = new String("");
//		if (iNodeId == 1) {
//			for (int i = 0; i < numNeighbors; i++) {
//				int neighbor = neighbors.get(i);
//				
//				String temp = new String("[");
//				synchronized(allTableLock) {
//					for (int j = 0; j < numNeighbors; j++) {
//						temp += neighbors.get(j);
//						temp += ":" + minCostNextHopRecommendation[i][j];
//						temp += ":" + nextHopMinCost[i][j] + ", ";
//					}
//				}
//				temp += "]";
//				neigh += " " + neighbor + temp;
//			}
//
//			String pretty_output = "{" + iNodeId + "} " + neigh;
//			System.out.println(pretty_output);
//		}		
		
	}
	
	public void run() {
		boolean done = false;

		// start a thread, that listens on port (iCoordinatorPort + iNodeId), to look-out for routing updates
		RoutingUpdateServerThread rust = new RoutingUpdateServerThread(iCoordinatorPort + iNodeId, iNodeId, numNodes, numNeighbors, this);
		rust.start();
		//System.out.println(iNodeId + " started RUST at port " + (iCoordinatorPort + iNodeId));

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
				// wait for all the nodes to join!
				while(numConnected < (numNodes -1)) {
					Socket incoming = ss.accept();
					numConnected++;
					// co-ordinator assigns node id to the connecting end-point
					ClientHandlerThread worker = new ClientHandlerThread(incoming, this, numNodes);
					worker.start();
				}
				ss.close();

				// wait for all workers to exit.
				while (numJoins < (numNodes -1)) {
					try {
						semAllJoined.acquire();
					} catch (InterruptedException ie) {
						ie.printStackTrace();
					}
				}
				
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
		
		//System.out.println(iNodeId + " going into sending mode.");
		boolean bDone = false;
		double routingBandwidth = 0;
		
//		if (iNodeId == 1) {
//			String pretty_output = "{" + iNodeId + "} #Neighbors = " + numNeighbors + ", ";
//			for (int i = 0; i < numNeighbors; i++) {
//				pretty_output += neighbors.get(i) + ":";
//			}
//			System.out.println(pretty_output);
//		}		

		// SEND ALL NEIGHBORS OUR ADJ TABLE
		while(!bDone) {
			// All nodes must have joined the overlay by now
			// Irrespective of whether the node is a co-ordinator or not, exchange adjacency tables.
			for (int i = 0; i < numNeighbors; i++) {
				int neighbor = neighbors.get(i);
				if (neighbor != iNodeId) {
					// send node i stuff on port (cPort + i)
					// TODO :: FIX_HACK :: i_ip should be not hardcoded.
					
					try {
						String i_ip = InetAddress.getLocalHost().getHostAddress();
						InetAddress i_ia = InetAddress.getLocalHost();
						//System.out.println(iNodeId + " trying to send adjecency table to " + i_ip + ":" + (iCoordinatorPort + neighbor));
						
						RoutingMsg rm = new RoutingMsg(iNodeId, numNodes);
						synchronized(allTableLock) {
							rm.populateProbeTable(probeTable[iNodeId]);
						}
						byte[] b = RoutingMsg.getBytes(rm);
						DatagramPacket dp = new DatagramPacket(b, b.length, i_ia, (iCoordinatorPort + neighbor)); 
						DatagramSocket s = new DatagramSocket();
						s.send(dp);
	
						routingBandwidth += b.length;
						s.close();
					} catch(Exception e) {
						System.out.println(iNodeId + ":" + (iCoordinatorPort + neighbor));
						e.printStackTrace();
					}
				}
			}
			
			bDone = true;
		}
		
		
		// wait for RUST to get all msgs and quit
		try {
			semDone.acquire();
		} catch (InterruptedException ie) {
			
		}

		updateBestHopRecommendation();

		// debug
//		String neigh = new String("");
//		if (iNodeId == 1) {
//			for (int i = 0; i < numNeighbors; i++) {
//				int neighbor = neighbors.get(i);
//				
//				String temp = new String("[");
//				synchronized(allTableLock) {
//					for (int j = 0; j < numNeighbors; j++) {
//						temp += neighbors.get(j);
//						temp += ":" + minCostNextHopRecommendation[i][j];
//						temp += ":" + nextHopMinCost[i][j] + ", ";
//					}
//				}
//				temp += "]";
//				neigh += " " + neighbor + temp;
//			}
//
//			String pretty_output = "{" + iNodeId + "} " + neigh;
//			System.out.println(pretty_output);
//		}		

		// NOW WE ARE SURE THAT ALL THE NODES HAVE COMPUTED THEIR RECOMMENDATIONS
		
//		System.out.println("{" + iNodeId + "} Starting Phase 2 ...");
		
		// START PHASE 2
		// restart a thread, that listens on port (iCoordinatorPort + iNodeId), to look-out for recommendation updates
		RecoUpdateServerThread rust1 = new RecoUpdateServerThread(iCoordinatorPort + iNodeId, iNodeId, numNodes, numNeighbors, this);
		rust1.start();

		// make sure everyone opens their sockets
		try {
			//System.out.println(iNodeId + " sleeping ...");
			Thread.sleep(5000);
			//System.out.println(iNodeId + " woke-up ...");
		} catch (InterruptedException ie) {

		}

		
		int[] neighborTableArray = new int[numNeighbors];
		for (int n = 0; n < numNeighbors; n++) {
			neighborTableArray[n] = neighbors.get(n);
		}
		
		// SEND ALL NEIGHBORS RECOMMENDATIONS
//		System.out.println("{" + iNodeId + "} Sending all neighbors next hop recommendations ...");
		bDone = false;
		while(!bDone) {
			for (int i = 0; i < numNeighbors; i++) {
				int neighbor = neighbors.get(i);
				if (neighbor != iNodeId) {
					
					try {
						String i_ip = InetAddress.getLocalHost().getHostAddress();
						InetAddress i_ia = InetAddress.getLocalHost();
						
						HitsGraphBestHopMsg msg = new HitsGraphBestHopMsg(iNodeId, numNeighbors);
						synchronized(allTableLock) {
							msg.populateNeighborTable(neighborTableArray);
							msg.populateBestHopeTable(minCostNextHopRecommendation[i]);
							msg.populateBestHopCostTable(nextHopMinCost[i]);
						}
						//System.out.println(iNodeId + " trying to send to " + i_ip + ":" + (iCoordinatorPort + neighbor) + ". " + msg);
						byte[] b = HitsGraphBestHopMsg.getBytes(msg);
						DatagramPacket dp = new DatagramPacket(b, b.length, i_ia, (iCoordinatorPort + neighbor)); 
						DatagramSocket s = new DatagramSocket();
						s.send(dp);
	
						routingBandwidth += b.length;
						s.close();
					} catch(Exception e) {
						System.out.println(iNodeId + ":" + (iCoordinatorPort + neighbor));
						e.printStackTrace();
					}
				}
			}
			
			bDone = true;
		}

//		System.out.println("{" + iNodeId + "} Done sending all neighbors next hop recommendations, waiting for routing thread to quit");
		// wait for RUST to get all msgs and quit
		try {
			semDone.acquire();
		} catch (InterruptedException ie) {
			
		}
		
		if (iNodeId == 0) {
			//String pot = "{" + iNodeId + "} Routing Banwidth Overhead = " + routingBandwidth + " bytes, numNodes = " + numNodes;
			String pot = numNodes + "\t" + routingBandwidth;
			System.out.println(pot);
		}
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
			//System.out.println(numJoins.intValue() + " nodes joined the overlay!");
			if (numJoins == (numNodes -1)) {
				semAllJoined.release();
			}
		}
	}
	
	public int getCoordinatorPort() {
		return iCoordinatorPort;
	}
	
	public void updateRoutingTable(RoutingMsg rm) {
		synchronized(allTableLock) {
			//System.out.println(printForwardingTable());
			rm.getProbeTable(probeTable[rm.getOriginatorId()]);
			//updateBestHopRecommendation(rm);
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
	
	private void updateBestHopRecommendation() {

		synchronized(allTableLock) {
			
			for (int start_neighbor_offset = 0; start_neighbor_offset < numNeighbors; start_neighbor_offset++) {	
				int start_neighbor = neighbors.get(start_neighbor_offset);

				// find minCostPath rm_originator_neighbor -> my_other_neighbors(neighbor)
				for (int end_neighbor_offset = 0; end_neighbor_offset < numNeighbors; end_neighbor_offset++) {	
					int end_neighbor = neighbors.get(end_neighbor_offset);
				
					if (end_neighbor != start_neighbor) {
					
						// debug
//						if (iNodeId == 1) {
//							System.out.println("{" + iNodeId + "} nextHopMinCost[start_neighbor_offset][end_neighbor_offset] = " + nextHopMinCost[start_neighbor_offset][end_neighbor_offset]);
//						}
						
						if (nextHopMinCost[start_neighbor_offset][end_neighbor_offset] == -1) {
							// debug
//							if (iNodeId == 1) {
//								System.out.println("{" + iNodeId + "} nextHopMinCost[start_neighbor_offset][end_neighbor_offset] was -1. setting to " + probeTable[start_neighbor][end_neighbor]);
//							}
							nextHopMinCost[start_neighbor_offset][end_neighbor_offset] = probeTable[start_neighbor][end_neighbor];
							minCostNextHopRecommendation[start_neighbor_offset][end_neighbor_offset] = start_neighbor;
						}
						
						int minCost = nextHopMinCost[start_neighbor_offset][end_neighbor_offset];
						int minCostNextHop = minCostNextHopRecommendation[start_neighbor_offset][end_neighbor_offset];
		
						// debug
//						if (iNodeId == 1) {
//							System.out.println("{" + iNodeId + "} from " + start_neighbor + "(offset = " + start_neighbor_offset + ") to " + end_neighbor + "(offset = " + end_neighbor_offset + ")");
//							System.out.println("{" + iNodeId + "} nextHopMinCost[start_neighbor_offset][end_neighbor_offset] = " + minCost);
//							System.out.println("{" + iNodeId + "} minCostNextHopRecommendation[start_neighbor_offset][end_neighbor_offset] = " + minCostNextHop);
//						}
						
						for(int i = 0; i < numNodes; i++) {
							if ( (i != start_neighbor) && (i != end_neighbor) ) {
								// if it is cheaper to reach end_neighbor from start_neighbor via i, modify minCost and next hop
								if ( minCost > (probeTable[start_neighbor][i] + probeTable[end_neighbor][i]) ) {
									// debug
//									if (iNodeId == 1) {
//										System.out.println("{" + iNodeId + "} hop(i) = " + i + ", *** [minCost > (probeTable[start_neighbor][i] + probeTable[end_neighbor][i])] = " + minCost + ", " + probeTable[start_neighbor][i] + ", " + probeTable[end_neighbor][i]);
//									}
									minCost = probeTable[start_neighbor][i] + probeTable[end_neighbor][i];
									minCostNextHop = i;
								}
							}
						}
						nextHopMinCost[start_neighbor_offset][end_neighbor_offset] = minCost;
						minCostNextHopRecommendation[start_neighbor_offset][end_neighbor_offset] = minCostNextHop;
	
						// debug
//						if (iNodeId == 1) {
//							System.out.println("{" + iNodeId + "} post calc"); 
//							System.out.println("{" + iNodeId + "} from " + start_neighbor + "(offset = " + start_neighbor_offset + ") to " + end_neighbor + "(offset = " + end_neighbor_offset + ")");
//							System.out.println("{" + iNodeId + "} nextHopMinCost[start_neighbor_offset][end_neighbor_offset] = " + minCost);
//							System.out.println("{" + iNodeId + "} minCostNextHopRecommendation[start_neighbor_offset][end_neighbor_offset] = " + minCostNextHop);
//						}
					} //if
				}
			}// for
		}// synch
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
