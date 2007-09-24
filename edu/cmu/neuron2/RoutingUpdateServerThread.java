package edu.cmu.neuron2;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import edu.cmu.neuron2.msg.BaseMsg;
import edu.cmu.neuron2.msg.RoutingMsg;

public class RoutingUpdateServerThread extends Thread {

	int iPort;
	int iNodeId;
	IRonNode parentHandle;
	boolean bQuit;
	Semaphore semDone;

	RoutingUpdateServerThread(int port, int node_id, IRonNode rn) {
		iPort = port;
		iNodeId = node_id;
		parentHandle = rn;

		bQuit = false;

		semDone = new Semaphore(0);
	}

	public void run() {
		try {

			DatagramSocket ds = new DatagramSocket(iPort);

			byte[] b = new byte[65536];
			DatagramPacket dp = new DatagramPacket(b, b.length);

			Hashtable<Integer, Long> lastTimes = new Hashtable<Integer, Long>();

			while (!bQuit) {
				// System.out.println(iNodeId + " RUST listening on port " +
				// iPort);
				ds.setSoTimeout(1000);
				try {
					ds.receive(dp);
					// System.out.println(iNodeId + " RUST - incoming adjecency
					// table!");
					// System.out.println(iNodeId + " RUST - incoming table!");

					// TODO :: check that the length is the same as b.length
					byte[] msg = dp.getData();
					ByteArrayInputStream bis = new ByteArrayInputStream(msg);
					DataInputStream dis = new DataInputStream(bis);
					int type = dis.readInt();
					dis.close();
					bis.close();

					if (type == BaseMsg.ROUTING_MSG_TYPE) {
						RoutingMsg rm1 = RoutingMsg.getObject(msg);
						lastTimes.put(rm1.getId(), System.currentTimeMillis());
						// TODO :: do something with the mesg
						System.out.println("Node " + iNodeId + "=> "
								+ rm1.toString());
					} else {
						System.out.println("UNKNOWN MSG type " + type
								+ " in RoutingUpdateServerThread");
					}

				} catch (SocketTimeoutException ste) {
					// if missed 3 periods, they're dead
					for (Entry<Integer, Long> entry : lastTimes.entrySet())
						if (System.currentTimeMillis() - entry.getValue() >= RoutingUpdateThread.TIMEOUT)
							parentHandle.removeMemberNode(entry.getKey());
				}
			}
			ds.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(iNodeId + " RoutingUpdateServerThread quitting.");
		semDone.release();
	}

	public void quit() {
		bQuit = true;
		semDone.acquireUninterruptibly();
	}
}
