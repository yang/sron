package edu.cmu.neuron2;

import edu.cmu.neuron2.msg.InitMsg;
import edu.cmu.neuron2.msg.MembershipMsg;
import edu.cmu.neuron2.msg.RoutingMsg;

public interface IRonNode {

	/*
	 * IRonNode implementations should create an initial msg containing a membership list.
	 * This method is supposed to call im.populateMemberList
	 * currently called from ClientHandlerThread
	 */
	public void populateMemberList(InitMsg im);

	public void aquireStateLock();
	public void releaseStateLock();
	public void addMemberNode(int node_id);
	public void removeMemberNode(int node_id);

	public void handleMembershipChange(MembershipMsg mm);
	
	public void sendAllNeighborsAdjacencyTable();
	public void sendAllNeighborsRoutingRecommendations();
	public void updateNetworkState(RoutingMsg rm);
	
	public void quit();
}
