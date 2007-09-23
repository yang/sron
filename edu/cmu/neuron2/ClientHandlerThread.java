package edu.cmu.neuron2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;

import edu.cmu.neuron2.msg.InitMsg;

public class ClientHandlerThread extends Thread {

	Socket incoming;
	IRonNode parent;
	int nodeId;
	int otherEndPointNodeId;
	InitMsg im;

	String otherEndPointIp;

	public ClientHandlerThread(Socket connection, IRonNode my_parent, int node_id) {
		incoming = connection;
		parent = my_parent;
		nodeId = node_id;
		otherEndPointIp ="";
	}
	
	public void run() {
		try {

			BufferedReader reader = new BufferedReader(new InputStreamReader(incoming.getInputStream()));
			ObjectOutputStream writer = new ObjectOutputStream(incoming.getOutputStream());
			
			String msg = new String("");
			boolean done = false;
			while (!done) {
			    String in_msg = reader.readLine();
			    if (in_msg != null) {
			    	done = true;
				    msg += in_msg;
			    }
			}

			if (msg.startsWith("join")) {
				String [] temp = null;
				temp = msg.split(" ");
				otherEndPointIp = temp[1];
				//int nid = Integer.parseInt(temp[2]);
				im = new InitMsg(nodeId);
			}
			
			parent.aquireStateLock();		// lock state (expensive locking operation)
			parent.addNode(nodeId);			// also sends out updates to other nodes!
			parent.populateMemberList(im);
			writer.writeObject(im);

			msg = new String("");
			done = false;
			while (!done) {
			    String in_msg = reader.readLine();
			    if (in_msg != null) {
			    	done = true;
				    msg += in_msg;
			    }
			}
			
			//System.out.println("Done!");
			reader.close();
			writer.close();
			incoming.close();
			parent.releaseStateLock(); 		// unlock state
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
