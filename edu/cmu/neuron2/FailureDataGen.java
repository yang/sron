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
            Random generator3 = new Random(System.currentTimeMillis() / 2);

            int numFailures = (failurePercent * numNodes * (numNodes -1)) / 100;

            for (int i = 0; i < numFailures; i++) {
                int randomSrc = generator.nextInt(numNodes) + 1;
                int randomDst = generator.nextInt(numNodes) + 1;

                float startTimeInSec = generator2.nextInt(runTime);
                float duration = generator2.nextInt(runTime);

                // this is just one model
                // there is no justification as to why this failure model is important.
                // Although there are overlapping failures here, it might be interesting to see
                // how the system responds to f% of overlapping failures

                // also the duration of failures should ideally be picked from a distribution that is seen in the real world.

                // so the best thing to do would probably be just apply real world data,
                // but this is a starting point till we have real world failure data.

                System.out.println(randomSrc + " " + randomDst + " " + startTimeInSec + " " + (startTimeInSec + duration));
                //System.out.println(randomSrc + " " + randomDst + " " + startTimeInSec + " " + runTime);

            }

        }

    }

}
