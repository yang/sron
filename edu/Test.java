package edu;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.ThreadModel;
import org.apache.mina.transport.socket.nio.DatagramAcceptor;
import sun.nio.ch.DatagramSocketAdaptor;

public class Test {

    public static void main(String[] args) throws Exception {
        InetAddress addr = InetAddress.getLocalHost();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DatagramAcceptor a = new DatagramAcceptor(executor);
        a.getDefaultConfig().setThreadModel(ThreadModel.MANUAL);
        for (int i = 0; i < 2; i++) {
            a.bind(new InetSocketAddress(addr, 10000 + i),
                    new IoHandlerAdapter() {
                        public void messageReceived(IoSession session, Object obj)
                                throws Exception {
                            System.out.println("received " + obj + " on " + session.getLocalAddress() + " in thread " + Thread.currentThread().getName());
                        }
                    });
        }
        System.out.println("hello");
        while (true) {
            Thread.sleep(1000);
            byte[] buf = new byte[2];
            DatagramSocket s = new DatagramSocket();
            s.send(new DatagramPacket(buf, buf.length, addr, 10000));
            s.send(new DatagramPacket(buf, buf.length, addr, 10001));
        }
    }
}
