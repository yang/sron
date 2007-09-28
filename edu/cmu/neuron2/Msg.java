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

        public String toString() {
            String s = new String("");
            s += "Node Id: " + id + ". Msg(" + version + ") = {";
            for (int i = 0; i < members.size(); i++) {
                s += members.get(i).id + ", ";
            }
            s += "}";
            return s;
        }
    }

    public static final class Membership extends Msg {
        public ArrayList<NodeInfo> members;
        public int numNodes;

        public String toString() {
            String s = new String("Membership Msg (" + version +
                                  ") from Node " + src + ". Msg = [");
            for (int i = 0; i < numNodes; i++) {
                s += members.get(i).id + ", ";
            }
            s += "]";
            return s;
        }
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
        public long ping_time;
        public long pong_time;
    }

    public static final class Measurements extends Msg {
        public ArrayList<Integer> membershipList;
        public long[] probeTable;
    }

    public static final class MemberPoll extends Msg {
    }

    public static final class PeeringRequest extends Msg {
    }

}