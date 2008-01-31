import java.nio.channels.DatagramChannel;

class ServerDataEvent {
	public NioServer server;
	public DatagramChannel socket;
	public byte[] data;

	public ServerDataEvent(NioServer server, DatagramChannel socket, byte[] data) {
		this.server = server;
		this.socket = socket;
		this.data = data;
	}
}