package edu.cmu.neuron2;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReactorTest {
    ExecutorService e;

    public ReactorTest() {
        e = Executors.newCachedThreadPool();
    }

    public void spawn(final Runnable r) {
        e.submit(new Runnable() {
            public void run() {
                try {
                    r.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void test() throws Exception {
        InetAddress localhost = InetAddress.getLocalHost();
        int serverPort = 11111, clientPort = 22222;
        final InetSocketAddress serverSa, clientSa;
        serverSa = new InetSocketAddress(localhost, serverPort);
        clientSa = new InetSocketAddress(localhost, clientPort);

        final ReactorHandler handler = new ReactorHandler() {
            public void handle(InetSocketAddress src, ByteBuffer buf) {
                System.out.println("received: " + buf);
            }
        };

        spawn(new Runnable() {
            public void run() {
                try {
                    Reactor r = new Reactor();
                    r.register(null, serverSa, handler);
                    Thread.sleep(1000);
                    r.react();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        spawn(new Runnable() {
            public void run() {
                try {
                    Reactor r = new Reactor();
                    ByteBuffer writeBuf = ByteBuffer.allocate(5);
                    ReactorService s = r.register(null, clientSa, handler);
                    Thread.sleep(2000);
                    s.send(writeBuf, clientSa);
                    r.react();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        spawn(new Runnable() {
            public void run() {
                try {
                    byte[] writeBuf = new byte[] {0, 1, 2, 3};
                    Thread.sleep(3000);
                    new DatagramSocket().send(new DatagramPacket(writeBuf, writeBuf.length, serverSa));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
    }

    public static void main(String args[]) throws Exception {
        new ReactorTest().test();
    }

}
