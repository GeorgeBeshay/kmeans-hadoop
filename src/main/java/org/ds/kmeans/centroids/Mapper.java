package org.ds.kmeans.centroids;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.ds.kmeans.Utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, IntWritable, Text> {
    // <LongWritable, Text, IntWritable, Text>
    // <KI, VI, KO, VO> = <?, DataPointsDimensions, Centroid, Dimensions>

    private List<List<Double>> centroids = new ArrayList<>();

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        centroids = Utility.parseCentroidsFromStringAttribute(job);
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
        String[] dataPoints = value.toString().split("\n");

        for (String dataPoint : dataPoints) {
            List<Double> dimensions = Utility.parseFeatureVector(dataPoint);
            int closestCentroidIdx = Utility.findClosestCentroid(dimensions, this.centroids);

            outputCollector.collect(new IntWritable(closestCentroidIdx), new Text(dataPoint));
        }
    }

}
