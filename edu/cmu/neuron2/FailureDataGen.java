package edu.cmu.neuron2;

import java.util.Random;

public class FailureDataGen {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java FailureDataGen failureRate(%) numNodes runTime");
            System.exit(0);
        } else {
            int failurePercent = Integer.parseInt(args[0]);
            int numNodes = Integer.parseInt(args[1]);
            int runTime = Integer.parseInt(args[2]);

            // select <src, dst> pair to fail at a given time
            // there nC2 * 2 links in all (directed graph), we have to pick
            // (failurePercent * n * (n -1)) / 100 of those to fail

            Random generator = new Random(System.currentTimeMillis());
            Random generator2 = new Random(System.currentTimeMillis() + 1000);

            int numFailures = (failurePercent * numNodes * (numNodes -1)) / 100;

            for (int i = 0; i < numFailures; i++) {
                int randomSrc = generator.nextInt(numNodes) + 1;
                int randomDst = generator.nextInt(numNodes) + 1;

                float timeInSec = generator2.nextInt(runTime);

                System.out.println(randomSrc + " " + randomDst + " " + timeInSec + " " + runTime);
            }

        }

    }

}
