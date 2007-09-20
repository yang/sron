package edu.cmu.neuron2.msg;

import java.io.Serializable;

public class InitMsg implements Serializable {
	int id;
	int[] memberList;
	int numNodes;
	
	public InitMsg(int identifier, int num_nodes) {
		id = identifier;
		memberList = new int[num_nodes];
		numNodes = num_nodes;
	}

	public void populateMemberList(Integer[] ml) {
		//String s = new String("");
		for (int i = 0; i < numNodes; i++) {
			memberList[i] = ml[i];
			//s += ", " + memberList[i];
		}
		//System.out.println(s);
	}
	
	public void getMemberList(int[] ml) {
		for (int i = 0; i < numNodes; i++) {
			ml[i] = memberList[i];
		}
	}
	
	public String toString() {
		String s = new String("");
		s += "Id: " + id + "\n";
		for (int i = 0; i < numNodes; i++) {
			s += ", " + memberList[i];
		}
		return s;
	}
	
	public int getId() {
		return id;
	}
	
}
