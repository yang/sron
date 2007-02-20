package edu.cmu.nuron;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Semaphore;

import edu.cmu.nuron.msg.InitMsg;

public class ClientHandlerThread extends Thread {

	Socket incoming;
	RonNode parent;
	int numNodes;
	int otherEndPointNodeId;
	InitMsg im;

	String otherEndPointIp;

	Semaphore semDone;
	
	public ClientHandlerThread(Socket connection, RonNode my_parent, int num_nodes, Semaphore sem_done) {
		incoming = connection;
		parent = my_parent;
		numNodes = num_nodes;
		otherEndPointIp ="";
		semDone = sem_done;
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
				int nid = Integer.parseInt(temp[2]);
				im = new InitMsg(nid, numNodes);
			}
			
			parent.populateInitProbeTable(im);
			// send intial probe table to the other end-point
			writer.writeObject(im);

			parent.doneJoiningOverlay();
			
//			// co-ordinator releases this once all the folks have connected to it.
//			try {
//				semDone.acquire();
//			} catch (InterruptedException ie) {
//				
//			}

		    //System.out.println("Done!");
			reader.close();
			writer.close();
			incoming.close();

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
