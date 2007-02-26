package edu.cmu.nuron.msg;

import java.io.Serializable;

public class InitMsg implements Serializable {
	int id;
	int[] probeTable;
	int numNodes;
	
	public InitMsg(int identifier, int num_nodes) {
		id = identifier;
		probeTable = new int[num_nodes];
		numNodes = num_nodes;
		for (int i = 0; i < numNodes; i++) {
			probeTable[i] = -1;
		}
	}

	public void populateProbeTable(int[] pt) {
		//String s = new String("");
		for (int i = 0; i < numNodes; i++) {
			probeTable[i] = pt[i];
			//s += ", " + probeTable[i];
		}
		//System.out.println(s);
	}
	
	public void getProbeTable(int[] p) {
		for (int i = 0; i < numNodes; i++) {
			p[i] = probeTable[i];
		}
	}
	
	public String toString() {
		String s = new String("");
		for (int i = 0; i < numNodes; i++) {
			s += ", " + probeTable[i];
		}
		return s;
	}
	
	public int getId() {
		return id;
	}
	
}
