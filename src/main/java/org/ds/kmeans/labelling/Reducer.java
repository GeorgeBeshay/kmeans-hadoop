package org.ds.kmeans.labelling;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class Reducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<IntWritable, Text, Text, IntWritable> {
    @Override
    public void reduce(IntWritable clusterIdx, Iterator<Text> dataPointsIterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        while (dataPointsIterator.hasNext()) {
            outputCollector.collect(dataPointsIterator.next(), clusterIdx);
        }
    }
}
