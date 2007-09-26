import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoServiceConfig;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.LoggingFilter;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.nio.DatagramAcceptor;
import org.apache.mina.transport.socket.nio.DatagramAcceptorConfig;
import org.apache.mina.transport.socket.nio.DatagramConnector;

public class MinaUdpExample {

	private static final int PORT = 9123;

	public static void main(String[] args) throws Exception {
		final IoServiceConfig cfg = new DatagramAcceptorConfig();
		cfg.getFilterChain().addLast("logger", new LoggingFilter());
		cfg.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));

		new DatagramAcceptor().bind(new InetSocketAddress(PORT),
				new TimeServerHandler(), cfg);
		System.out.println("server started");

		new DatagramConnector().connect(
				new InetSocketAddress("localhost", PORT),
				new TimeClientHandler(), cfg);
		System.out.println("client started");
	}

	public static class TimeClientHandler extends IoHandlerAdapter {
		@Override
		public void sessionCreated(IoSession session) throws Exception {
			session.write(new ArrayList<String>());
		}
	}

	public static class TimeServerHandler extends IoHandlerAdapter {
		public void exceptionCaught(IoSession session, Throwable t)
				throws Exception {
			t.printStackTrace();
			session.close();
		}

		@Override
		public void messageReceived(IoSession session, Object msg)
				throws Exception {
			System.out.println("server got " + msg);
		}

		@Override
		public void sessionCreated(IoSession session) throws Exception {
			System.out.println("server session created");
			session.setIdleTime(IdleStatus.BOTH_IDLE, 10);
		}
	}
}