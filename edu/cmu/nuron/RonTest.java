package edu.cmu.nuron;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class RonTest {

	protected static String sExpInterfaceIp;
	protected int iNumNodes;
	protected boolean bCoOrdinator;
	protected String sCoOrdinatorServerName;
	protected int iCoOrdinatorServerPort;
	protected boolean bUseQuorum;

	static {
		//expInterfaceIp = getExperimentalAddress();

		// hack for machines with only 1 interface
		sExpInterfaceIp = "localhost";
	}
	
	private static String getExperimentalAddress() {
		String retVal = null;
		String expIpPrefix = "172.23.65.";
		try {
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			while(en.hasMoreElements()) {
				NetworkInterface ni = en.nextElement();
				Enumeration<InetAddress> en2 = ni.getInetAddresses();
				while(en2.hasMoreElements()) {
					InetAddress ia = en2.nextElement();
					String ip = ia.getHostAddress();
					if (ip.startsWith(expIpPrefix)) {
						retVal = ip;
						return retVal;
					}
					/*
					System.out.println("Interface name:" + ni.getName() +
						" display name:" + ni.getDisplayName() + " " + ip);
					*/
				}
			}
		} catch (SocketException se) {
			se.printStackTrace();
		}
		return retVal;
	}
	
	RonTest() {
	}

	public static void main(String[] args) throws Exception {
		
		sExpInterfaceIp = InetAddress.getLocalHost().getHostAddress();
		//System.out.println("IP = " + sExpInterfaceIp);
		if (sExpInterfaceIp != null) {
			RonTest rn = new RonTest();

			if (args.length != 4) {
	        	System.out.println("Usage: java RonTest numNodes CoOrdinatorServerName CoOrdinatorServerPort bUseQuorum");
	            System.exit(0);
	        } else {
				rn.iNumNodes = Integer.parseInt(args[0]);
				rn.sCoOrdinatorServerName = args[1];
				rn.iCoOrdinatorServerPort = Integer.parseInt(args[2]);
				rn.bUseQuorum = Boolean.parseBoolean(args[3]);

				//rn.sCoOrdinatorServerName = "localhost";
				//rn.iCoOrdinatorServerPort = 8000;
	        }
			
			//rn.printProperties();
			
			
			if (rn.bUseQuorum) {
				if (!rn.isPerfectSquare(rn.iNumNodes)) {
					System.out.println("Note :: Number of nodes should be a perfect square.");
					System.exit(0);
				}

				for (int i = 0; i < rn.iNumNodes; i++) {
					NeuRonNode node = new NeuRonNode(i, rn.sCoOrdinatorServerName, rn.iCoOrdinatorServerPort, rn.iNumNodes);
					node.start();
				}
			}
			else {
				for (int i = 0; i < rn.iNumNodes; i++) {
					RonNode node = new RonNode(i, rn.sCoOrdinatorServerName, rn.iCoOrdinatorServerPort, rn.iNumNodes);
					node.start();
				}
			}
		}
	}
	
	private void printProperties() {
		System.out.println("numNodes = " + iNumNodes);
		System.out.println("Co-ordinator Endpoint = " + sCoOrdinatorServerName + ":" + iCoOrdinatorServerPort);

		/*
		if (bCoOrdinator) {
			System.out.println("Member is a co-ordinator!");
		} else {
			System.out.println("Member is not a co-ordinator!");
		}
		*/
		
		if (bUseQuorum) {
			System.out.println("Using Grid Quorum!");
		} else {
			System.out.println("Using Full Mesh!");
		}
	}
	
	private boolean isPerfectSquare(int num) {
		int sq_root = (int) Math.sqrt(num);
		int square = sq_root * sq_root;
		
		if (square == num) {
			return true;
		}
		return false;
	}
	
}
