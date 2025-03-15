package org.ds.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Utility {

    public static List<List<Double>> parseCentroidsFromStringAttribute(JobConf job) {
        List<List<Double>> centroids = new ArrayList<>();
        String[] lines = job.get("centroids").split("\n");
        for (String line : lines) {
            centroids.add(parseFeatureVector(line));
        }
        return centroids;
    }

    public static String convertCentroidsToString(List<List<Double>> centroids) {
        if (centroids == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < centroids.size(); i++) {
            List<Double> centroid = centroids.get(i);
            for (int j = 0; j < centroid.size(); j++) {
                sb.append(centroid.get(j));
                if (j != centroid.size() - 1) {
                    sb.append(",");
                }
            }
            if (i != centroids.size() - 1) {
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    public static List<List<Double>> loadCentroidsFromFile(String directoryPath) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path path = new Path(directoryPath);
        FileStatus[] fileStatuses = fs.listStatus(path);
        List<List<Double>> centroids = new ArrayList<>();

        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isFile() || !fileStatus.getPath().getName().startsWith("part-")) {
                continue;
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.split("\t")[1];
                String[] dimensions = line.split(",");
                List<Double> centroid = new ArrayList<>();
                for (String dimension : dimensions) {
                    centroid.add(Double.parseDouble(dimension.trim()));
                }
                centroids.add(centroid);
            }
            reader.close();
        }

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

    public static List<List<Double>> prepareInitialCentroids(String inputFile, int centroidsCount) throws IOException {

        FileSystem fs = FileSystem.get(new Configuration());
        Path path = new Path(inputFile);
        List<List<Double>> centroids = new ArrayList<>();

        // Open the file
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            int rowCount = 0;

            // Read the file line by line
            while ((line = reader.readLine()) != null && rowCount < centroidsCount) {
                centroids.add(parseFeatureVector(line));
                rowCount++;
            }
        }

        return centroids;
    }

    public static void removeIfExists(String path1, String path2) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(path1))) {
            fs.delete(new Path(path1), true);
        }
        if (fs.exists(new Path(path2))) {
            fs.delete(new Path(path2), true);
        }
    }

}
