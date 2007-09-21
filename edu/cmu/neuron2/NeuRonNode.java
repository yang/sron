package edu.cmu.neuron2;

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

import edu.cmu.neuron2.msg.HitsGraphBestHopMsg;
import edu.cmu.neuron2.msg.InitMsg;
import edu.cmu.neuron2.msg.RoutingMsg;

public class NeuRonNode extends Thread implements IRonNode {
	int iNodeId;
	boolean bCoordinator;
	String sCoordinatorIp;
	int iCoordinatorPort;

	boolean done;
	
	MembershipUpdateServerThread must; 	
	RoutingUpdateServerThread rust;

	ArrayList<Integer> members;
	//ArrayList<Integer> neighbors;
	
	public NeuRonNode(int id, String cName, int cPort) {
		iNodeId = id;
		sCoordinatorIp = cName;
		iCoordinatorPort = cPort;
		
		done = false;
		
		must = null;
		rust = null;
		
		members = new ArrayList<Integer>();
		
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
					members.add(new Integer(numConnected));
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
				readInMemberList(im);
				
				// start a thread, that listens on port 2*(iCoordinatorPort + iNodeId), to look-out for routing updates
				must = new MembershipUpdateServerThread((iCoordinatorPort + 1000) + iNodeId, iNodeId, this);
				must.start();
				//System.out.println(iNodeId + " started RUST at port " + (iCoordinatorPort + iNodeId));

				// start a thread, that listens on port (iCoordinatorPort + iNodeId), to look-out for routing updates
				rust = new RoutingUpdateServerThread(iCoordinatorPort + iNodeId, iNodeId, this);
				rust.start();
				//System.out.println(iNodeId + " started RUST at port " + (iCoordinatorPort + iNodeId));

				reader.close();
				writer.close();
				s.close();
				
			} catch(Exception e){
				System.out.println("Could not connect or connection was interrupted.");
				e.printStackTrace();
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

	public int getCoordinatorPort() {
		return iCoordinatorPort;
	}
	
	public void quit() {
		done = true;
		
		if (must != null) {
			must.quit();
		}
		
		if (rust != null) {
			rust.quit();
		}
	}
}
