package edu.cmu.neuron2.msg;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public abstract class BaseMsg {
	int msgType;
	
	public static final int MEMBERSHIP_MSG_TYPE = 0;
	public static final int ROUTING_MSG_TYPE = 1;
	public static final int BESTHOP_RECOMMENDATION_MSG_TYPE = 2;
	

	public static BaseMsg getObject(byte[] b) throws Exception {
	    ByteArrayInputStream bis = new ByteArrayInputStream(b);
	    DataInputStream dis = new DataInputStream(bis);
	    int msgType = dis.readInt();
	    dis.close();
	    bis.close();
	    
	    BaseMsg bm = null;
	    if (msgType == BaseMsg.ROUTING_MSG_TYPE) {
	    	bm = RoutingMsg.getObject(b);
	    }
	    else if (msgType == BaseMsg.BESTHOP_RECOMMENDATION_MSG_TYPE) {
	    	bm = HitsGraphBestHopMsg.getObject(b);
	    }
	    return bm;
	}

}
