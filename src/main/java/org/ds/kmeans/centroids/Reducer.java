package org.ds.kmeans.centroids;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

// Global reducer. It reduces all the partial results being passed from each local combiner
// to compute the final centroids.
public class Reducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable centroidIdx, Iterator<Text> partialDimensions, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
        double[] currentCentroid = null;
        int dataPointsCount = 0;

        while (partialDimensions.hasNext()) {

            String partialDimension = partialDimensions.next().toString();
            String[] dimensions = partialDimension.split(",");

            // the first element is the data points count.
            dataPointsCount += Integer.parseInt(dimensions[0].trim());

            if (currentCentroid == null) {
                currentCentroid = new double[dimensions.length - 1];
            }
            for (int i = 0; i < dimensions.length - 1; i++) {
                currentCentroid[i] += Double.parseDouble(dimensions[i + 1].trim()); // pass the first element
            }
        }

        StringBuilder centroidString = new StringBuilder();
        for (int i = 0; i < Objects.requireNonNull(currentCentroid).length; i++) {
            centroidString.append(currentCentroid[i] / dataPointsCount).append(",");
        }

        outputCollector.collect(centroidIdx, new Text(centroidString.toString()));
    }
}
