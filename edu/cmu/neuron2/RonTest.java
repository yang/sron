package edu.cmu.neuron2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class RonTest {

    public static enum RunMode { SIM, DIST }

    private Semaphore semAllJoined;

    public RonTest() {
        semAllJoined = new Semaphore(0);
    }

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
        rt.run();
    }

    public void run() throws Exception {

        Properties props = System.getProperties();
        String config = System.getProperty("neuron.config");
        if (config != null) {
            props.load(new FileInputStream(config));
        }

        final ExecutorService executor = Executors.newCachedThreadPool();
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        final List<NeuRonNode> nodes = new ArrayList<NeuRonNode>();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                for (NeuRonNode node : nodes) {
                    node.quit();
                }
                executor.shutdown();
            }
        });
        int numNodes = Integer.parseInt(props.getProperty("numNodes", "3"));
        int nodeId = Integer.parseInt(props.getProperty("nodeId", "0"));
        RunMode mode = RunMode.valueOf(props.getProperty("mode", "sim").toUpperCase());
        String simData = props.getProperty("simData", "");

        switch (mode) {
        case SIM:
            for (int i = 0; i <= numNodes; i++) {
                NeuRonNode node = new NeuRonNode(i, executor, scheduler,
                                                props, numNodes, semAllJoined);
                node.start();
                nodes.add(node);
            }
            semAllJoined.acquire();
            System.out.println("All aboard !!!!!");
            sim(simData, nodes, scheduler);
            break;
        case DIST:
            NeuRonNode node = new NeuRonNode(nodeId, executor, scheduler,
                                            props, numNodes, semAllJoined);
            node.start();
            nodes.add(node);
            break;
        }
    }

    private void sim(String datafile, final List<NeuRonNode> nodes,
        ScheduledExecutorService scheduler) throws IOException {
        if (!datafile.equals("")) {
            BufferedReader reader = new BufferedReader(new FileReader(datafile));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                final int src = Integer.parseInt(parts[0]);
                final int dst = Integer.parseInt(parts[1]);
                double startTime = Double.parseDouble(parts[2]);
                double stopTime = Double.parseDouble(parts[3]);
                final ScheduledFuture<?> future = scheduler.schedule(new Runnable() {
                    public void run() {
                        nodes.get(dst).ignore(src);
                    }
                }, (long) (startTime * 1000), TimeUnit.MILLISECONDS);
                scheduler.schedule(new Runnable() {
                    public void run() {
                        if (!future.cancel(false)) {
                            nodes.get(dst).unignore(src);
                        }
                    }
                }, (long) (stopTime * 1000), TimeUnit.MILLISECONDS);
            }
        }
    }

}
