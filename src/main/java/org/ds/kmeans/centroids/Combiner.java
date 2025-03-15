package org.ds.kmeans.centroids;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

// Performs local aggregation. Same as the global reducer, but performs the partial reduction
// on the data generated as output from each map task.
public class Combiner extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable centroidIdx, Iterator<Text> dataPoints, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
        double[] currentCentroid = null;
        int dataPointsCount = 0;

        while (dataPoints.hasNext()) {
            dataPointsCount++;

            String dataPoint = dataPoints.next().toString();
            String[] tempDimensions = dataPoint.split(",");

            if (currentCentroid == null) {
                currentCentroid = new double[tempDimensions.length];
            }
            for (int i = 0; i < tempDimensions.length; i++) {
                currentCentroid[i] += Double.parseDouble(tempDimensions[i]);
            }
        }

        StringBuilder centroidString = new StringBuilder();
        // put the data points count as the first element in this string.
        centroidString.append(dataPointsCount);

        // finally, append the partial centroid dimensions.
        for (int i = 0; i < Objects.requireNonNull(currentCentroid).length; i++) {
            centroidString.append(currentCentroid[i]).append(",");
        }

        outputCollector.collect(centroidIdx, new Text(centroidString.toString()));
    }
}
