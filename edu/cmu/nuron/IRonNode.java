package edu.cmu.nuron;

import edu.cmu.nuron.msg.InitMsg;
import edu.cmu.nuron.msg.RoutingMsg;

//TODO :: revisit this interface and look at the methods it offers carefully (so that they make sense)
public interface IRonNode {

	// each IRonNode implementation should have a RUST - this method is called by RUST when it quits
	public void routingThreadQuit();

	/* When a node joins the overlay, it claims that it has an id.
	 * IRonNode implementations should create an initial msg containing ther adj table for that node.
	 * This method is supposed to call im.populateProbeTable
	 * currently called from ClientHandlerThread
	 */
	public void populateInitProbeTable(InitMsg im);

	// called from the ClientHandlerThread when a node completes joining an overlay (ie. when the InitMsg is sent to it).
	public void doneJoiningOverlay();

	/* RUST is started before nodes join the overlay, and once the nodes join the overlay they start transmitting updates (adj table).
	 * But this could be dangerous as all nodes might not have received their InitMsg, and processing a RoutingMsg before InitMsg is dangerous.
	 * So we wait for all nodes to get their Initmsg, before we process the RoutingMsg
	 * called by RoutingUpdateHandlerThread
	 */
	public void requestPermissionToProcessRoutingUpdate();

	/* Update the Routing Table state based on this update received
	 * and also trigger a Forwarding Table update
	 */
	public void updateRoutingTable(RoutingMsg rm);

}
