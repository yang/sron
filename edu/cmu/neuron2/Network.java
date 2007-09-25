package edu.cmu.neuron2;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

/**
 * Provides a common network subsystem to give us control over packet
 * filtering/failure injection.
 * 
 * This class is thread-safe.
 */
public class Network {

	private final DatagramSocket socket;

	public Network() {
		try {
			socket = new DatagramSocket();
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void send(DatagramPacket p) {
		try {
			socket.send(p);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public void close() {
		socket.close();
	}

}
