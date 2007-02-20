package edu.cmu.nuron;

import java.io.DataInputStream;
import java.net.Socket;
import java.util.concurrent.Semaphore;

import edu.cmu.nuron.msg.RoutingMsg;

public class RoutingUpdateHandlerThread extends Thread {
	
	Socket incoming;
	int parentNodeId;
	int sizeOfRoutingUpdate;
	byte []routingUpdate;
	
	RonNode parentHandle;
	Semaphore completionSemaphore;

	RoutingUpdateHandlerThread(Socket s, int parent_node_id, int size_of_routing_update, RonNode rn, Semaphore completion_semaphore) {
		incoming = s;
		parentNodeId = parent_node_id;
		sizeOfRoutingUpdate = size_of_routing_update;
		routingUpdate = new byte[sizeOfRoutingUpdate];
		parentHandle = rn;
		completionSemaphore = completion_semaphore;
	}

	public void run() {
		try {
			
			DataInputStream reader = new DataInputStream(incoming.getInputStream());
			// Read the RoutingMsg off the socket
			reader.readFully(routingUpdate, 0, sizeOfRoutingUpdate);
			RoutingMsg rm = RoutingMsg.getObject(routingUpdate);

			parentHandle.requestPermissionToProcessRoutingUpdate();
			//System.out.println("{" + parentNodeId + "} " + rm.toString());
			parentHandle.updateRoutingTable(rm);

			// clean-up
			reader.close();
			incoming.close();
			completionSemaphore.release();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
