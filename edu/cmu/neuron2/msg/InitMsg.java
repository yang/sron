package edu.cmu.neuron2.msg;

import java.io.Serializable;
import java.util.ArrayList;

public class InitMsg implements Serializable {
	int id;
	int[] memberList;
	int numNodes;
	
	public InitMsg(int identifier) {
		id = identifier;
		memberList = null;
		numNodes = 0;
	}

	public void initMemberList(ArrayList<Integer> ml) {
		if (ml != null) {
			numNodes = ml.size();
			memberList = new int[numNodes];
			for (int i = 0; i < numNodes; i++) {
				memberList[i] = ml.get(i);
			}
		}
	}
	
	public void getMemberList(ArrayList<Integer> ml) {
		if (ml != null) {
			for (int i = 0; i < numNodes; i++) {
				ml.add(new Integer(memberList[i]));
			}
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
