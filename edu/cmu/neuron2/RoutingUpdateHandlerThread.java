package edu.cmu.neuron2;

import java.util.concurrent.Semaphore;

import edu.cmu.neuron2.msg.RoutingMsg;

public class RoutingUpdateHandlerThread extends Thread {
	
	int parentNodeId;
	RoutingMsg rm;
	IRonNode parentHandle;
	Semaphore completionSemaphore;

	RoutingUpdateHandlerThread(RoutingMsg routing_msg, int parent_node_id, IRonNode rn, Semaphore completion_semaphore) {
		rm = routing_msg;
		parentNodeId = parent_node_id;
		parentHandle = rn;
		completionSemaphore = completion_semaphore;
	}

	public void run() {
		parentHandle.requestPermissionToProcessRoutingUpdate();
		
//		if (parentNodeId == 0) {
//			System.out.println("{" + parentNodeId + "} " + rm.toString());
//		}
		parentHandle.updateRoutingTable(rm);

		completionSemaphore.release();
	}

}
