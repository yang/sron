package edu.cmu.neuron2;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Semaphore;

import edu.cmu.neuron2.msg.BaseMsg;
import edu.cmu.neuron2.msg.RoutingMsg;

public class RoutingUpdateServerThread extends Thread {
	
	int iPort;
	int iNodeId;
	boolean bQuit;
	
	int sizeOfRoutingUpdate;
	
	IRonNode parentHandle;
	
	RoutingUpdateServerThread(int port, int node_id, IRonNode rn) {
		iPort = port;
		iNodeId = node_id;
		bQuit = false;
		sizeOfRoutingUpdate = 0;
		parentHandle = rn;
	}

	public void run() {
		try {
			
			DatagramSocket ds = new DatagramSocket(iPort);

			Semaphore completionSemaphore = new Semaphore(0);
			
			int i = 0;

			RoutingMsg rm = new RoutingMsg(iNodeId, numNodes);
			byte []b = RoutingMsg.getBytes(rm);
			sizeOfRoutingUpdate = b.length;
			DatagramPacket dp = new DatagramPacket(b, b.length);
			int j = 0;

			while (!bQuit) {
				//System.out.println(iNodeId + " RUST listening on port " + iPort);
				ds.setSoTimeout(1000);
				try {
					ds.receive(dp);
					i++;
					//System.out.println(iNodeId + " RUST - incoming adjecency table!");
					//System.out.println(iNodeId + " RUST - incoming table!");
	
						// TODO :: check that the length is the same as b.length
						byte[] msg = dp.getData();
					    ByteArrayInputStream bis = new ByteArrayInputStream(msg);
					    DataInputStream dis = new DataInputStream(bis);
					    int type = dis.readInt();
						dis.close();
						bis.close();

						if (type == BaseMsg.ROUTING_MSG_TYPE) {
							RoutingMsg rm1  = RoutingMsg.getObject(msg);
							RoutingUpdateHandlerThread ruht = new RoutingUpdateHandlerThread(rm1, iNodeId, parentHandle, completionSemaphore);
							ruht.start();
						}
						else {
							System.out.println("UNKNOWN MSG type " + type + " in RoutingUpdateServerThread");
						}
					
				} catch (SocketTimeoutException ste) {
					
				} finally {
					j++;
				}
			}
			ds.close();
			

			//System.out.println(iNodeId + " -----> " + i);
			// you should quit only when all your threads are done.
			try {
				completionSemaphore.acquire(i);
			} catch(InterruptedException ie) {
				
			}

		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
