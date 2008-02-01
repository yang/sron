package edu.cmu.neuron2;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReactorTest {
    ExecutorService e;

    public ReactorTest() {
        e = Executors.newFixedThreadPool(2);
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
        int serverPort = 10000, clientPort = 10001;
        final InetSocketAddress serverSa, clientSa;
        serverSa = new InetSocketAddress(localhost, serverPort);
        clientSa = new InetSocketAddress(localhost, clientPort);

        final Reactor s = new Reactor();
        spawn(new Runnable() {
            public void run() {
                try {
                    s.react(clientSa, serverSa);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
        spawn(new Runnable() {
            public void run() {
                try {
                    s.react(serverSa, null);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
    }

    public static void main(String args[]) throws Exception {
        new ReactorTest().test();
    }

}
