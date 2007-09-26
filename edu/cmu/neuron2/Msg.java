package edu.cmu.neuron2;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;

public class Msg implements Serializable {
	
	public int src;
	public int version;

	public static final class Join extends Msg {
		public InetAddress addr;
	}

	public static final class Init extends Msg {
		public int id;
		public ArrayList<NodeInfo> members;
	}

	public static final class Membership extends Msg {
		public ArrayList<NodeInfo> members;
		public int numNodes;
	}

	public static final class RoutingRecs extends Msg {
		public static final class Rec implements Serializable {
			public int dst;
			public int via; // the hop

			public Rec(int dst, int via) {
				this.dst = dst;
				this.via = via;
			}
		}

		public ArrayList<Rec> recs;
	}

	public static final class Ping extends Msg {
		/**
		 * a local timestamp
		 */
		public long time;
		/**
		 * info about the origin of the ping; this is how nodes keep each other
		 * informed of their existence
		 */
		public NodeInfo info;
	}
	
	public static final class Pong extends Msg {
		public long time;
	}

	public static final class Measurements extends Msg {
		public ArrayList<Integer> membershipList;
		public int[] probeTable;
	}
	
	public static final class MemberPoll extends Msg {
	}

}