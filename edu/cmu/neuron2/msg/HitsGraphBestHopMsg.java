package edu.cmu.neuron2.msg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class HitsGraphBestHopMsg extends BaseMsg {
	int id; // routing msg from node with this id
	
//	 id of the neighbor in the hits graph, best hop to that neighbor, cost.
	int[] neighborTable;
	int[] bestHopTable;
	int[] bestHopCostTable;
	
	int numNeighbors;

	public HitsGraphBestHopMsg(int identifier, int num_neighbors) {
		msgType = BaseMsg.BESTHOP_RECOMMENDATION_MSG_TYPE;
		
		id = identifier;
		numNeighbors = num_neighbors;
		neighborTable = new int[numNeighbors];
		bestHopTable = new int[numNeighbors];
		bestHopCostTable = new int[numNeighbors];
	}

	public void populateNeighborTable(int[] nt) {
		for (int i = 0; i < numNeighbors; i++) {
			neighborTable[i] = nt[i];
		}
	}

	public void getNeighborTable(int[] n) {
		for (int i = 0; i < numNeighbors; i++) {
			n[i] = neighborTable[i];
		}
	}

	public void populateBestHopeTable(int[] bht) {
		for (int i = 0; i < numNeighbors; i++) {
			bestHopTable[i] = bht[i];
		}
	}

	public void getBestHopTable(int[] bht) {
		for (int i = 0; i < numNeighbors; i++) {
			bht[i] = bestHopTable[i];
		}
	}

	public void populateBestHopCostTable(int[] bhct) {
		for (int i = 0; i < numNeighbors; i++) {
			bestHopCostTable[i] = bhct[i];
		}
	}

	public void getBestHopCostTable(int[] bhct) {
		for (int i = 0; i < numNeighbors; i++) {
			bhct[i] = bestHopCostTable[i];
		}
	}

	public String toString() {
		String s = new String("BestHopMsg Msg from Node " + id + ". Msg = [");
		for (int i = 0; i < numNeighbors; i++) {
			s += neighborTable[i] + ":" + bestHopTable[i] + ":" + bestHopCostTable[i] + ", ";
		}
		s += "]";
		return s;
	}
	
	public static byte[] getBytes(HitsGraphBestHopMsg msg) throws java.io.IOException{
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(bos);
	    dos.writeInt(msg.msgType);
	    dos.writeInt(msg.id);
	    dos.writeInt(msg.numNeighbors);
	    for(int i = 0; i < msg.numNeighbors; i++) {
	    	dos.writeInt(msg.neighborTable[i]);
	    	dos.writeInt(msg.bestHopTable[i]);
	    	dos.writeInt(msg.bestHopCostTable[i]);
	    }
	    dos.flush();
	    dos.close();
	    bos.close();
	    byte [] data = bos.toByteArray();
	    return data;
	}

	public static HitsGraphBestHopMsg getObject(byte[] b) throws Exception {
	    ByteArrayInputStream bis = new ByteArrayInputStream(b);
	    DataInputStream dis = new DataInputStream(bis);

	    int msgType = dis.readInt();

	    HitsGraphBestHopMsg msg = null;
	    if (msgType == BaseMsg.BESTHOP_RECOMMENDATION_MSG_TYPE) {
		    int id = dis.readInt();
		    int numNeighbors = dis.readInt();
		    msg = new HitsGraphBestHopMsg(id, numNeighbors);
		    for(int i = 0; i < msg.numNeighbors; i++) {
		    	msg.neighborTable[i] = dis.readInt();
		    	msg.bestHopTable[i] = dis.readInt();
		    	msg.bestHopCostTable[i] = dis.readInt();
		    }
	    }

	    dis.close();
	    bis.close();
	    return msg;
	}
}
