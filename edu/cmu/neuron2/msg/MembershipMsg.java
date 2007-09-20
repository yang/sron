package edu.cmu.neuron2.msg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/*
 * Represents Adjecency/Probe Table for a node identified by the field "id".
 */
public class MembershipMsg extends BaseMsg {
	int id; // routing msg from node with this id
	int[] membershipList;
	int numNodes;

	static 	ByteArrayOutputStream baos = new ByteArrayOutputStream();

	public MembershipMsg(int identifier, int num_nodes) {
		msgType = BaseMsg.MEMBERSHIP_MSG_TYPE;
		id = identifier;
		membershipList = new int[num_nodes];
		numNodes = num_nodes;
		for (int i = 0; i < numNodes; i++) {
			membershipList[i] = -1;
		}
	}

	public void populateMembershipList(int[] mt) {
		for (int i = 0; i < numNodes; i++) {
			membershipList[i] = mt[i];
		}
	}
	
	public void getMembershipList(int[] ml) {
		for (int i = 0; i < numNodes; i++) {
			ml[i] = membershipList[i];
		}
	}
	
	public int getOriginatorId() {
		return id;
	}
	
	public String toString() {
		String s = new String("Routing Msg from Node " + id + ". Msg = [");
		for (int i = 0; i < numNodes; i++) {
			s += membershipList[i] + ", ";
		}
		s += "]";
		return s;
	}

	public static byte[] getBytes(MembershipMsg rm) throws java.io.IOException{
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(bos);
	    dos.writeInt(rm.msgType);
	    dos.writeInt(rm.id);
	    dos.writeInt(rm.numNodes);
	    for(int i = 0; i < rm.numNodes; i++) {
	    	dos.writeInt(rm.membershipList[i]);
	    }
	    dos.flush();
	    dos.close();
	    bos.close();
	    byte [] data = bos.toByteArray();
	    return data;
	}

	public static MembershipMsg getObject(byte[] b) throws Exception {
	    ByteArrayInputStream bis = new ByteArrayInputStream(b);
	    DataInputStream dis = new DataInputStream(bis);
	    int msgType = dis.readInt();
	    int id = dis.readInt();
	    int numNodes = dis.readInt();
	    MembershipMsg rm = new MembershipMsg(id, numNodes);
	    for(int i = 0; i < rm.numNodes; i++) {
	    	rm.membershipList[i] = dis.readInt();
	    }
	    dis.close();
	    bis.close();
	    return rm;
	}
}
