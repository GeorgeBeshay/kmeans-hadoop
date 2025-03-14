package org.ds;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
    // <LongWritable, Text, IntWritable, Text>
    // <KI, VI, KO, VO> = <?, DataPointsDimensions, Centroid, Dimensions>

    private List<List<Double>> centroids = new ArrayList<>();

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        try {
            loadCentroidsFromFile(job.get("centroidsFp"), job);
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
        String[] dataPoints = value.toString().split("\n");

        for (String dataPoint : dataPoints) {
            String[] dimensionStrings = dataPoint.split(",");

            List<Double> dimensions = new ArrayList<>();
            for (String dimensionString : dimensionStrings) {
                dimensions.add(Double.parseDouble(dimensionString.trim()));
            }
            int closestCentroidIdx = this.findClosestCentroid(dimensions);

            outputCollector.collect(new IntWritable(closestCentroidIdx), new Text(dataPoint));
        }
    }

    private int findClosestCentroid(List<Double> point) {
        int closestCentroid = -1;
        double currentMinDistance = Double.MAX_VALUE;

        for (int i = 0; i < this.centroids.size(); i++) {
            double tempDistance = this.computeEuclideanDistance(this.centroids.get(i), point);
            if (tempDistance < currentMinDistance) {
                closestCentroid = i;
                currentMinDistance = tempDistance;
            }
        }

        return closestCentroid;
    }

    private double computeEuclideanDistance(List<Double> featureVector1, List<Double> featureVector2) {
        assert featureVector1.size() == featureVector2.size();

        double squaredResult = 0.0;
        for (int i = 0; i < featureVector1.size(); i++) {
            squaredResult += Math.pow(featureVector1.get(i) - featureVector2.get(i), 2);
        }
        return Math.sqrt(squaredResult);
    }

    private void loadCentroidsFromFile(String filePath, JobConf job) throws IOException {
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(job);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;

        while ((line = reader.readLine()) != null) {
            String[] dimensions = line.split(",");
            List<Double> centroid = new ArrayList<>();
            for (String dimension : dimensions) {
                centroid.add(Double.parseDouble(dimension.trim()));
            }
            centroids.add(centroid);
        }
        reader.close();
    }
}
