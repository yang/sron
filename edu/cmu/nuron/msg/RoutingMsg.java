package edu.cmu.nuron.msg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

public class RoutingMsg implements Serializable {
	int id; // routing msg from node with this id
	int[] probeTable;
	int numNodes;

	static 	ByteArrayOutputStream baos = new ByteArrayOutputStream();

	public RoutingMsg(int identifier, int num_nodes) {
		id = identifier;
		probeTable = new int[num_nodes];
		numNodes = num_nodes;
		for (int i = 0; i < numNodes; i++) {
			probeTable[i] = -1;
		}
	}

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
	
	public int getOriginatorId() {
		return id;
	}
	
	public String toString() {
		String s = new String("Routing Msg from Node " + id + ". Msg = [");
		for (int i = 0; i < numNodes; i++) {
			s += probeTable[i] + ", ";
		}
		s += "]";
		return s;
	}

	public static byte[] getBytes(RoutingMsg rm) throws java.io.IOException{
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(bos);
	    dos.writeInt(rm.id);
	    dos.writeInt(rm.numNodes);
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
	    int id = dis.readInt();
	    int numNodes = dis.readInt();
	    RoutingMsg rm = new RoutingMsg(id, numNodes);
	    for(int i = 0; i < rm.numNodes; i++) {
	    	rm.probeTable[i] = dis.readInt();
	    }
	    dis.close();
	    bis.close();
	    return rm;
	}
}
