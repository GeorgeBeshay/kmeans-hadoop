package org.ds.kmeans;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Utility {
    public static List<List<Double>> loadCentroidsFromFile(JobConf job) throws IOException {

        // case 1 - the initial job should work using  randomly initialized centroids.
        boolean isInitialJob = job.getBoolean("isInitialJob", false);
        if (isInitialJob) {
            return generateRandomCentroids(
                    job.getInt("numberOfCentroids", 3),
                    job.getInt("featureVectorSize", 4)
            );
        }

        // case 2 - work using the previous job centroids.
        Path path = new Path(job.get("centroidsFp"));
        FileSystem fs = path.getFileSystem(job);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

        List<List<Double>> centroids = new ArrayList<>();
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

        return centroids;
    }

    public static double computeEuclideanDistance(List<Double> featureVector1, List<Double> featureVector2) {
        assert featureVector1.size() == featureVector2.size();

        double squaredResult = 0.0;
        for (int i = 0; i < featureVector1.size(); i++) {
            squaredResult += Math.pow(featureVector1.get(i) - featureVector2.get(i), 2);
        }
        return Math.sqrt(squaredResult);
    }

    public static int findClosestCentroid(List<Double> point, List<List<Double>> centroids) {
        int closestCentroid = -1;
        double currentMinDistance = Double.MAX_VALUE;

        for (int i = 0; i < centroids.size(); i++) {
            double tempDistance = Utility.computeEuclideanDistance(centroids.get(i), point);
            if (tempDistance < currentMinDistance) {
                closestCentroid = i;
                currentMinDistance = tempDistance;
            }
        }

        return closestCentroid;
    }

    public static boolean isConverged(List<List<Double>> oldCentroids, List<List<Double>> newCentroids, double threshold) {
        assert oldCentroids.size() == newCentroids.size();

        boolean converged = true;
        for (int i = 0; i < oldCentroids.size(); i++) {
            if (computeEuclideanDistance(oldCentroids.get(i), newCentroids.get(i)) > threshold) {
                converged = false;
                break;
            }
        }

        return converged;
    }

    public static List<Double> parseFeatureVector(String featureVector) {
        List<Double> featureVectorList = new ArrayList<>();
        String[] dims = featureVector.split(",");

        for (String dim : dims) {
            featureVectorList.add(Double.parseDouble(dim.trim()));
        }

        return featureVectorList;
    }

    public static List<List<Double>> generateRandomCentroids(int centroidsCount, int featureVectorSize) {
        List<List<Double>> centroids = new ArrayList<>();
        for (int i = 0; i < centroidsCount; i++) {
            List<Double> centroid = new ArrayList<>();
            for (int j = 0; j < featureVectorSize; j++) {
                centroid.add(Math.random());
            }
            centroids.add(centroid);
        }
        return centroids;
    }

}
