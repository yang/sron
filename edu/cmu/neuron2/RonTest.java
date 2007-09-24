package edu.cmu.neuron2;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class RonTest {

	private static enum RunMode { SIM, DIST }

	public static void main(String[] args) throws Exception {

		final List<IRonNode> nodes = new ArrayList<IRonNode>();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				for (IRonNode node : nodes) {
					node.quit();
				}
			}
		});
		String sExpInterfaceIp = "localhost";
		int iNumNodes = 0;
		int iNodeNum = 0;
		String sCoOrdinatorServerName;
		int iCoOrdinatorServerPort;
		RunMode mode;
		
		sExpInterfaceIp = InetAddress.getLocalHost().getHostAddress();
		// System.out.println("IP = " + sExpInterfaceIp);
		if (sExpInterfaceIp != null) {
			try {
				if (args[0].equalsIgnoreCase("sim")) {
					mode = RunMode.SIM;
					iNumNodes = Integer.parseInt(args[1]);
					sCoOrdinatorServerName = args[2];
					iCoOrdinatorServerPort = Integer.parseInt(args[3]);
				} else if (args[0].equalsIgnoreCase("dist")) {
					mode = RunMode.DIST;
					iNodeNum = Integer.parseInt(args[1]);
					sCoOrdinatorServerName = args[2];
					iCoOrdinatorServerPort = Integer.parseInt(args[3]);
				} else {
					throw new RuntimeException();
				}
			} catch (Exception ex) {
				System.out
						.println("Usage: java RonTest sim numNodes CoOrdinatorServerName CoOrdinatorServerPort");
				System.out
						.println("Usage: java RonTest dist nodeId CoOrdinatorServerName CoOrdinatorServerPort");
				System.exit(1);
				return;
			}

			switch (mode) {
			case SIM:
				for (int i = 0; i <= iNumNodes; i++) {
					NeuRonNode node = new NeuRonNode(i,
							sCoOrdinatorServerName,
							iCoOrdinatorServerPort);
					node.start();
					nodes.add(node);
				}
				break;
			case DIST:
				NeuRonNode node = new NeuRonNode(iNodeNum,
						sCoOrdinatorServerName, iCoOrdinatorServerPort);
				node.start();
				nodes.add(node);
				break;
			}
		}
	}

}
