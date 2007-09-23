package edu.cmu.neuron2.msg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;

public class RoutingMsg extends BaseMsg {
	int id; // routing msg from node with this id
	int[] membershipList;
	int[] probeTable;
	int numNodes;

	static 	ByteArrayOutputStream baos = new ByteArrayOutputStream();

	public RoutingMsg(int identifier, ArrayList<Integer> ml, ArrayList<Integer> pt) {
		msgType = BaseMsg.ROUTING_MSG_TYPE;
		id = identifier;

		numNodes = ml.size();
		membershipList = new int[numNodes];
		for (int i = 0; i < numNodes; i++) {
			membershipList[i] = ml.get(i);
		}

		probeTable = new int[numNodes];
		for (int i = 0; i < numNodes; i++) {
			probeTable[i] = pt.get(i);
		}

		/*
		for (int i = 0; i < numNodes; i++) {
			probeTable[i] = -1;
		}
		*/
	}

	private RoutingMsg(int identifier, int num_nodes) {
		msgType = BaseMsg.ROUTING_MSG_TYPE;
		id = identifier;
		numNodes = num_nodes;
		membershipList = new int[numNodes];
		probeTable = new int[numNodes];
	}
	
	/*
	public void populateProbeTable(int[] pt) {
		for (int i = 0; i < numNodes; i++) {
			probeTable[i] = pt[i];
		}
	}
	
	public void getProbeTable(int[] p) {
		for (int i = 0; i < numNodes; i++) {
			p[i] = probeTable[i];
		}
	}
	
	public int getProbeEntry(int offset) {
		if ((offset >= 0) && (offset < numNodes)) {
			return probeTable[offset];
		}
		return -1;
	}
	*/
	
	public int getOriginatorId() {
		return id;
	}
	
	public String toString() {
		String s = new String("Routing Msg from Node " + id + ". Msg = [");
		for (int i = 0; i < numNodes; i++) {
			s += membershipList[i] + ":" + probeTable[i] + ", ";
		}
		s += "]";
		return s;
	}

	public static byte[] getBytes(RoutingMsg rm) throws java.io.IOException{
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(bos);
	    dos.writeInt(rm.msgType);
	    dos.writeInt(rm.id);
	    dos.writeInt(rm.numNodes);
	    for(int i = 0; i < rm.numNodes; i++) {
	    	dos.writeInt(rm.membershipList[i]);
	    }
	    for(int i = 0; i < rm.numNodes; i++) {
	    	dos.writeInt(rm.probeTable[i]);
	    }
	    dos.flush();
	    dos.close();
	    bos.close();
	    byte [] data = bos.toByteArray();
	    return data;
	}

	public static RoutingMsg getObject(byte[] b) throws Exception {
	    ByteArrayInputStream bis = new ByteArrayInputStream(b);
	    DataInputStream dis = new DataInputStream(bis);
	    int msgType = dis.readInt();
	    int id = dis.readInt();
	    int numNodes = dis.readInt();
	    RoutingMsg rm = new RoutingMsg(id, numNodes);
	    for(int i = 0; i < rm.numNodes; i++) {
	    	rm.membershipList[i] = dis.readInt();
	    }
	    for(int i = 0; i < rm.numNodes; i++) {
	    	rm.probeTable[i] = dis.readInt();
	    }
	    dis.close();
	    bis.close();
	    return rm;
	}
}
