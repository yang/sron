package edu.cmu.neuron2;

import java.io.Serializable;
import java.net.InetAddress;

public class NodeInfo implements Serializable {
	public int port;
	public InetAddress addr;
	public int id;
}

