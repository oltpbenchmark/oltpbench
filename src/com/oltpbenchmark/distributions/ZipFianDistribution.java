package com.oltpbenchmark.distributions;


import java.util.Random;

public class ZipFianDistribution {
    private final Random rnd;
    private final int size;
    private final double skew;
    
    // XXX: should really be final
    private double bottom = 0;

    /** Uses rnd */
    public ZipFianDistribution(Random rnd, int size, double skew) {
        this.rnd = rnd;
        this.size = size;
        this.skew = skew;
        initBottom();
    }
    
    public ZipFianDistribution(int size, double skew) {
        this.rnd = new Random(System.currentTimeMillis());
        this.size = size;
        this.skew = skew;
        initBottom();
    }
    
    private void initBottom() {
        for (int i = 1; i <= size; i++) {
            this.bottom += (1 / Math.pow(i, this.skew));
        }
    }

    // the next() method returns an rank id. The frequency of returned rank ids
    // are follows Zipf distribution.
    public int next() {
        int rank;
        double frequency = 0;
        double dice;

        rank = rnd.nextInt(size);
        frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
        dice = rnd.nextDouble();

        while (!(dice < frequency)) {
            rank = rnd.nextInt(size);
            frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
            dice = rnd.nextDouble();
        }

        return rank;
    }

    // This method returns a probability that the given rank occurs.
    public double getProbability(int rank) {
        return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
    }
}
