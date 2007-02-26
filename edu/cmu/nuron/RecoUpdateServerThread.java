package edu.cmu.nuron;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Semaphore;

import edu.cmu.nuron.msg.BaseMsg;
import edu.cmu.nuron.msg.HitsGraphBestHopMsg;

public class RecoUpdateServerThread extends Thread {
	
	int iPort;
	int iNodeId;
	boolean bQuit;
	int numNodes;
	int numNeighbors;
	
	int sizeOfUpdate;
	
	IRonNode parentHandle;
	
	RecoUpdateServerThread(int port, int node_id, int num_nodes, int num_neighbors, IRonNode rn) {
		iPort = port;
		iNodeId = node_id;
		bQuit = false;
		numNodes = num_nodes;
		numNeighbors = num_neighbors;
		sizeOfUpdate = 0;
		parentHandle = rn;
	}

	public void run() {
		try {
			
			DatagramSocket ds = new DatagramSocket(iPort);

			Semaphore completionSemaphore = new Semaphore(0);
			
			int i = 0;

			HitsGraphBestHopMsg bhm = new HitsGraphBestHopMsg(iNodeId, numNeighbors);
			byte []b = HitsGraphBestHopMsg.getBytes(bhm);
			sizeOfUpdate = b.length;
			DatagramPacket dp = new DatagramPacket(b, b.length);
			int j = 0;

			while (!bQuit) {
				//System.out.println(iNodeId + " RUST listening on port " + iPort);
				ds.setSoTimeout(1000);
				try {
					ds.receive(dp);
					i++;
					//System.out.println(iNodeId + " RUST - incoming table!");
	
					// TODO :: check that the length is the same as b.length
					byte[] msg = dp.getData();
				    ByteArrayInputStream bis = new ByteArrayInputStream(msg);
				    DataInputStream dis = new DataInputStream(bis);
				    int type = dis.readInt();
					dis.close();
					bis.close();

					if (type == BaseMsg.BESTHOP_RECOMMENDATION_MSG_TYPE) {
						//System.out.println("{" + iNodeId + "} got msg of Type " + type + " and length " + msg.length);
						HitsGraphBestHopMsg bhm1  = HitsGraphBestHopMsg.getObject(msg);
						BestHopUpdateHandlerThread bhuht = new BestHopUpdateHandlerThread(bhm1, iNodeId, parentHandle, completionSemaphore);
						bhuht.start();
					}
					else {
						System.out.println("UNKNOWN MSG type " + type + " in RecoUpdateServerThread");
					}
					
					if ( i >= numNeighbors ) {
						// all nodes have sent their tables
						bQuit = true;
					}
				} catch (SocketTimeoutException ste) {
					
				} finally {
					j++;
					// max wait for 30 sec - then quit
//					if (j >= 30) {
//						bQuit = true;
//					}
				}
			}
			ds.close();
			

			//System.out.println(iNodeId + " -----> " + i);
			// you should quit only when all your threads are done.
			try {
				completionSemaphore.acquire(i);
			} catch(InterruptedException ie) {
				
			}
			
			//System.out.println(parentHandle.printForwardingTable());
			parentHandle.routingThreadQuit();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
