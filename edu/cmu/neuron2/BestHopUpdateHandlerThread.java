package edu.cmu.neuron2;

import java.util.concurrent.Semaphore;

import edu.cmu.neuron2.msg.HitsGraphBestHopMsg;

public class BestHopUpdateHandlerThread extends Thread {
	
	int parentNodeId;
	HitsGraphBestHopMsg bhm;
	IRonNode parentHandle;
	Semaphore completionSemaphore;

	BestHopUpdateHandlerThread(HitsGraphBestHopMsg best_hop_msg, int parent_node_id, IRonNode rn, Semaphore completion_semaphore) {
		bhm = best_hop_msg;
		parentNodeId = parent_node_id;
		parentHandle = rn;
		completionSemaphore = completion_semaphore;
	}

	public void run() {
		//parentHandle.requestPermissionToProcessRoutingUpdate();
		
//		if (parentNodeId == 7) {
//			System.out.println("{" + parentNodeId + "} " + bhm.toString());
//		}
			
		// TODO :: remember to ignore self in the msg 
		//parentHandle.updateRoutingTable(rm);

		completionSemaphore.release();
	}

}
