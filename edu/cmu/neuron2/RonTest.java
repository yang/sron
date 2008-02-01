package edu.cmu.neuron2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.mina.common.ThreadModel;
import org.apache.mina.transport.socket.nio.DatagramAcceptor;

import edu.cmu.neuron2.NeuRonNode.PlannedException;

public class RonTest {

    public static enum RunMode { SIM, DIST }

    public static void main(String[] args) throws Exception {

        /*
        Properties props = System.getProperties();
        String config = System.getProperty("neuron.config");
        if (config != null) {
            props.load(new FileInputStream(config));
        }
        int numNodes = Integer.parseInt(props.getProperty("numNodes", "3"));
        String filter = props.getProperty("logfilter");
        NeuRonNode.RoutingScheme scheme = NeuRonNode.RoutingScheme.valueOf(props.getProperty("scheme", "SIMPLE").toUpperCase());

        System.out.println("#nodes = " + numNodes + "; filter = " + filter + "; scheme = " + scheme);
        //System.exit(0);
        */

        RonTest rt = new RonTest();
        try {
            rt.run();
        } catch (PlannedException ex) {
        //} catch (Exception ex) {
        //     System.exit(7);
        }
    }

    public void run() throws Exception {

        Properties props = System.getProperties();
        String config = System.getProperty("neuron.config");
        if (config != null) {
            props.load(new FileInputStream(config));
        }

        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        final Reactor reactor = new Reactor();

        // periodically GC
        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                System.gc();
            }
        }, 1, 1, TimeUnit.MINUTES);

        final List<NeuRonNode> nodes = new ArrayList<NeuRonNode>();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                for (NeuRonNode node : nodes) {
                    node.quit();
                }
            }
        });
        short numNodes = Short.parseShort(props.getProperty("numNodes", "3"));
        short nodeId = Short.parseShort(props.getProperty("nodeId", "0"));
        RunMode mode = RunMode.valueOf(props.getProperty("mode", "sim").toUpperCase());
        final String simData = props.getProperty("simData", "");

        int basePort = Integer.parseInt(props.getProperty("basePort", mode == RunMode.SIM ? "15000" : "9000"));
        NodeInfo coordNode = new NodeInfo();
        coordNode.id = 0;
        // InetAddress.getLocalHost() fails when # of nodes is large - hence optimizing
        String coordinatorHost;
        try {
            coordinatorHost = props.getProperty("coordinatorHost",
                    InetAddress.getLocalHost().getHostAddress());
            coordNode.addr = InetAddress.getByName(coordinatorHost);
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
        coordNode.port = basePort;

        DatagramAcceptor acceptor = new DatagramAcceptor();
        acceptor.getDefaultConfig().setThreadModel(ThreadModel.MANUAL);

        Runnable watchdog = new Runnable() {
            public void run() {
                Logger.getLogger("").info("total time is up");
                reactor.shutdown();
                scheduler.shutdown();
                System.exit(0);
            }
        };

        Runnable semAllJoined = new Runnable() {
            public void run() {
                if (nodes.get(0).failure.get() != null) {
                    nodes.get(0).failure.get().printStackTrace();
                    System.exit(1);
                } else {
                    System.out.println("All aboard !!!!!");
                    sim(simData, nodes, reactor);
                }
            }
        };
        switch (mode) {
        case SIM:
            InetAddress myCachedAddr;
            try {
                myCachedAddr = InetAddress.getLocalHost();
            } catch (UnknownHostException ex) {
                throw new RuntimeException(ex);
            }

            for (short i = 0; i <= numNodes; i++) {
                NeuRonNode node = new NeuRonNode(i, props, numNodes,
                        i == 0 ? semAllJoined : null, myCachedAddr,
                        coordinatorHost, coordNode, acceptor, reactor);
                node.run();
                nodes.add(node);
            }

            break;
        case DIST:
            NeuRonNode node = new NeuRonNode(nodeId, props, numNodes,
                    semAllJoined, null, coordinatorHost, coordNode, acceptor, reactor);
            node.run();
            nodes.add(node);

            break;
        }

        int totalTime = Integer.parseInt(props.getProperty("totalTime", "60"));
        Logger.getLogger("").info((totalTime > 0 ? "" : "NOT ") +
                "scheduling total time watchdog");
        if (totalTime > 0) {
            reactor.schedule(watchdog, totalTime, TimeUnit.SECONDS);
        }

        reactor.react();
    }

    private void sim(String datafile, final List<NeuRonNode> nodes,
        Reactor scheduler) {
        try {
            if (!datafile.equals("")) {
                BufferedReader reader = new BufferedReader(new FileReader(datafile));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(" ");
                    final short src = Short.parseShort(parts[0]);
                    final short dst = Short.parseShort(parts[1]);
                    double startTime = Double.parseDouble(parts[2]);
                    double stopTime = Double.parseDouble(parts[3]);
                    final ScheduledFuture<?> future = scheduler.schedule(new Runnable() {
                        public void run() {
                            for (NeuRonNode node : nodes) {
                                if (node.myNid == src) {
                                    System.out.println(node.myNid + " : <" + src + ", " + dst + ">");
                                    node.ignore(dst);
                                } else if (node.myNid == dst) {
                                    System.out.println(node.myNid + " : <" + src + ", " + dst + ">");
                                    node.ignore(src);
                                }
                            }
                        }
                    }, (long) (startTime * 1000), TimeUnit.MILLISECONDS);
                    scheduler.schedule(new Runnable() {
                        public void run() {
                            if (!future.cancel(false)) {
                                for (NeuRonNode node : nodes) {
                                    if (node.myNid == src) {
                                        node.unignore(dst);
                                    }
                                    else if (node.myNid == dst) {
                                        node.unignore(src);
                                    }
                                }
                            }
                        }
                    }, (long) (stopTime * 1000), TimeUnit.MILLISECONDS);
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

}
