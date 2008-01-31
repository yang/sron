import java.nio.channels.DatagramChannel;

public class ChangeRequest {
	public static final int REGISTER = 1;
	public static final int CHANGEOPS = 2;

	public DatagramChannel socket;
	public int type;
	public int ops;

	public ChangeRequest(DatagramChannel socket, int type, int ops) {
		this.socket = socket;
		this.type = type;
		this.ops = ops;
	}
}
