package edu.cmu.neuron2.msg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;

/*
 * Represents Adjecency/Probe Table for a node identified by the field "id".
 */
public class MembershipMsg extends BaseMsg {
	int id; // membership msg from node with this id
	int[] membershipList;
	int numNodes;

	static 	ByteArrayOutputStream baos = new ByteArrayOutputStream();

	public MembershipMsg(int identifier, ArrayList<Integer> ml) {
		msgType = BaseMsg.MEMBERSHIP_MSG_TYPE;
		id = identifier;
		
		numNodes = ml.size();
		
		membershipList = new int[numNodes];
		for (int i = 0; i < numNodes; i++) {
			membershipList[i] = ml.get(i);
		}
	}

//	public void populateMembershipList(int[] mt) {
//		for (int i = 0; i < numNodes; i++) {
//			membershipList[i] = mt[i];
//		}
//	}

//	public void getMembershipList(int[] ml) {
//		for (int i = 0; i < numNodes; i++) {
//			ml[i] = membershipList[i];
//		}
//	}
	
	public void getMemberList(ArrayList<Integer> ml) {
		if (ml != null) {
			for (int i = 0; i < numNodes; i++) {
				ml.add(new Integer(membershipList[i]));
			}
		}
	}
	
	public int getOriginatorId() {
		return id;
	}
	
	public String toString() {
		String s = new String("Membership Msg from Node " + id + ". Msg = [");
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
	    ArrayList<Integer> ml = new ArrayList<Integer>();
	    for(int i = 0; i < numNodes; i++) {
	    	ml.add(dis.readInt());
	    }
	    MembershipMsg rm = new MembershipMsg(id, ml);
	    dis.close();
	    bis.close();
	    return rm;
	}
}
