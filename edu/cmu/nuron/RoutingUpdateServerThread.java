package edu.cmu.nuron;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Semaphore;

import edu.cmu.nuron.msg.RoutingMsg;

public class RoutingUpdateServerThread extends Thread {
	
	int iPort;
	int iNodeId;
	boolean bQuit;
	int numNodes;
	
	int sizeOfRoutingUpdate;
	
	RonNode parentHandle;
	
	RoutingUpdateServerThread(int port, int node_id, int num_nodes, RonNode rn) {
		iPort = port;
		iNodeId = node_id;
		bQuit = false;
		numNodes = num_nodes;
		sizeOfRoutingUpdate = 0;
		parentHandle = rn;
	}

	public void run() {
		try {
			
			RoutingMsg rm = new RoutingMsg(iNodeId, numNodes);
			byte []b = RoutingMsg.getBytes(rm);
			sizeOfRoutingUpdate = b.length;

			ServerSocket ss = new ServerSocket(iPort);
			ss.setReuseAddress(true);
			ss.setSoTimeout(1000); // 1 sec

			Semaphore completionSemaphore = new Semaphore(0);
			
			int i = 0;
			while (!bQuit) {
				try {
					//System.out.println(iNodeId + " RUST listening on port " + iPort);
					Socket incoming = ss.accept();
					i++;
					//System.out.println(iNodeId + " RUST - incoming adjecency table!");
					RoutingUpdateHandlerThread ruht = new RoutingUpdateHandlerThread(incoming, iNodeId, sizeOfRoutingUpdate, parentHandle, completionSemaphore);
					ruht.start();
					
					if ( i >= (numNodes - 1) ) {
						// all nodes have sent their tables
						bQuit = true;
					}
				} catch (SocketTimeoutException ste) {
				}
			}
			ss.close();

			// you should quit only when all your threads are done.
			try {
				completionSemaphore.acquire(numNodes -1);
			} catch(InterruptedException ie) {
				
			}
			
			System.out.println(parentHandle.printForwardingTable());
			parentHandle.routingThreadQuit();

		} catch(Exception e) {
			
		}
	}

}
