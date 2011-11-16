/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark;

import java.util.Arrays;

public class DistributionStatistics {
	private static final double[] PERCENTILES = { 0.0, 0.25, 0.5, 0.75, 0.9,
			0.95, 0.99, 1.0 };

	private static final int MINIMUM = 0;
	private static final int PERCENTILE_25TH = 1;
	private static final int MEDIAN = 2;
	private static final int PERCENTILE_75TH = 3;
	private static final int PERCENTILE_90TH = 4;
	private static final int PERCENTILE_95TH = 5;
	private static final int PERCENTILE_99TH = 6;
	private static final int MAXIMUM = 7;

	private final int count;
	private final long[] percentiles;
	private final double average;
	private final double standardDeviation;

	public DistributionStatistics(int count, long[] percentiles,
			double average, double standardDeviation) {
		assert count > 0;
		assert percentiles.length == PERCENTILES.length;
		this.count = count;
		this.percentiles = Arrays.copyOfRange(percentiles, 0,
				PERCENTILES.length);
		this.average = average;
		this.standardDeviation = standardDeviation;
	}

	/**
	 * Computes distribution statistics over values. WARNING: This will sort
	 * values.
	 */
	public static DistributionStatistics computeStatistics(int[] values) {
		if (values.length == 0) {
			//			throw new IllegalArgumentException(
			//					"cannot compute statistics for an empty list");
			
			long[] percentiles = new long[PERCENTILES.length];
			for (int i = 0; i < percentiles.length; ++i) {
				percentiles[i] = -1;
			}
			return new DistributionStatistics(values.length, percentiles, 0,0);
		
		}
		Arrays.sort(values);

		double sum = 0;
		for (int i = 0; i < values.length; ++i) {
			sum += values[i];
		}
		double average = sum / values.length;

		double sumDiffsSquared = 0;
		for (int i = 0; i < values.length; ++i) {
			double v = values[i] - average;
			sumDiffsSquared += v * v;
		}
		double standardDeviation = 0;
		if (values.length > 1) {
			standardDeviation = Math
					.sqrt(sumDiffsSquared / (values.length - 1));
		}

		// NOTE: NIST recommends interpolating. This just selects the closest
		// value, which is
		// described as another common technique.
		// http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm
		long[] percentiles = new long[PERCENTILES.length];
		for (int i = 0; i < percentiles.length; ++i) {
			int index = (int) (PERCENTILES[i] * values.length);
			if (index == values.length)
				index = values.length - 1;
			percentiles[i] = values[index];
		}

		return new DistributionStatistics(values.length, percentiles, average,
				standardDeviation);
	}

	public int getCount() {
		return count;
	}

	public double getAverage() {
		return average;
	}

	public double getStandardDeviation() {
		return standardDeviation;
	}

	public double getMinimum() {
		return percentiles[MINIMUM];
	}

	public double get25thPercentile() {
		return percentiles[PERCENTILE_25TH];
	}

	public double getMedian() {
		return percentiles[MEDIAN];
	}

	public double get75thPercentile() {
		return percentiles[PERCENTILE_75TH];
	}

	public double get90thPercentile() {
		return percentiles[PERCENTILE_90TH];
	}

	public double get95thPercentile() {
		return percentiles[PERCENTILE_95TH];
	}

	public double get99thPercentile() {
		return percentiles[PERCENTILE_99TH];
	}

	public double getMaximum() {
		return percentiles[MAXIMUM];
	}

	@Override
	public String toString() {
		// convert times to ms
		return "[min=" + getMinimum() / 1e6 + ", " + "25th="
				+ get25thPercentile() / 1e6 + ", " + "median="
				+ getMedian() / 1e6 + ", " + "avg=" + getAverage() / 1e6 + ", "
				+ "75th=" + get75thPercentile() / 1e6 + ", " + "90th="
				+ get90thPercentile() / 1e6 + ", " + "95th="
				+ get95thPercentile() / 1e6 + ", " + "99th="
				+ get99thPercentile() / 1e6 + ", " + "max=" + getMaximum()
				/ 1e6 + "]";
	}
}
