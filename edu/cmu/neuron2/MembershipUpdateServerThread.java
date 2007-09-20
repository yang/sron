package edu.cmu.neuron2;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Semaphore;

import edu.cmu.neuron2.msg.BaseMsg;
import edu.cmu.neuron2.msg.MembershipMsg;

public class MembershipUpdateServerThread extends Thread {
	
	int iPort;
	int iNodeId;
	boolean bQuit;
	
	int sizeOfRoutingUpdate;
	
	IRonNode parentHandle;
	
	MembershipUpdateServerThread(int port, int node_id, IRonNode rn) {
		iPort = port;
		iNodeId = node_id;
		bQuit = false;
		sizeOfRoutingUpdate = 0;
		parentHandle = rn;
	}

	public void run() {
		try {
			
			DatagramSocket ds = new DatagramSocket(iPort);

			int i = 0;

			MembershipMsg mm = new MembershipMsg(iNodeId, numNodes);
			byte []b = MembershipMsg.getBytes(mm);
			sizeOfRoutingUpdate = b.length;
			DatagramPacket dp = new DatagramPacket(b, b.length);
			int j = 0;

			while (!bQuit) {
				//System.out.println(iNodeId + " RUST listening on port " + iPort);
				ds.setSoTimeout(1000);
				try {
					ds.receive(dp);
					i++;
					//System.out.println(iNodeId + " RUST - incoming adjecency table!");
					//System.out.println(iNodeId + " RUST - incoming table!");
	
						// TODO :: check that the length is the same as b.length
						byte[] msg = dp.getData();
					    ByteArrayInputStream bis = new ByteArrayInputStream(msg);
					    DataInputStream dis = new DataInputStream(bis);
					    int type = dis.readInt();
						dis.close();
						bis.close();

						if (type == BaseMsg.MEMBERSHIP_MSG_TYPE) {
							MembershipMsg mm1  = MembershipMsg.getObject(msg);
							// TODO :: do something with the mesg
						}
						else {
							System.out.println("UNKNOWN MSG type " + type + " in RoutingUpdateServerThread");
						}
					
				} catch (SocketTimeoutException ste) {
					
				} finally {
					j++;
				}
			}
			ds.close();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
