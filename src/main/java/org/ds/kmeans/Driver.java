package org.ds.kmeans;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.ds.kmeans.centroids.Combiner;
import org.ds.kmeans.centroids.Mapper;
import org.ds.kmeans.centroids.Reducer;

import java.io.IOException;
import java.util.List;

public class Driver {

    static int MAX_ITERATIONS = 100;
    static double CONVERGENCE_THRESHOLD = 0.0001;

    /**
     *
     * @param args The command-line arguments passed to the program:
     *             - Data input filepath
     *             - Centroids output filepath
     *             - Data output (labels) filepath
     *             - Number of centroids
     * @throws IOException Problem had occurred while reading an input file during the job.
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 4) {
            System.err.println(
                    "Usage: KMeansParallelDriver" +
                    "\n\t<data_input_filepath>" +
                    "\n\t<centroids_output_filepath>" +
                    "\n\t<data_labels_output_filepath>" +
                    "\n\t<num_centroids>"
            );
            System.exit(1);
        }

        Utility.removeIfExists(args[1], args[2]);

        int currentIteration = 0;
        boolean converged = false;
        List<List<Double>> currentCentroids = null, newCentroids;

        while (currentIteration < MAX_ITERATIONS && !converged) {
            newCentroids = runCentroidsComputationJob(args, currentCentroids, currentIteration);
            if (currentIteration > 0) {
                converged = Utility.isConverged(currentCentroids, newCentroids, CONVERGENCE_THRESHOLD);
            }
            currentCentroids = newCentroids;
            currentIteration++;
        }

        runDataLabellingJob(args, currentCentroids);
    }

    public static List<List<Double>> runCentroidsComputationJob(
            String[] args,
            List<List<Double>> currentCentroids,
            int iterationId
    ) throws IOException {

        String centroidsFilePath = args[1] + "/" + "iteration" + iterationId;

        JobConf conf = new JobConf(Driver.class);

        conf.setJobName("KMeans_MapReduce_Centroids_Computation_" + iterationId);
        conf.set(
                "centroids",
                currentCentroids != null
                        ? Utility.convertCentroidsToString(currentCentroids)
                        : Utility.convertCentroidsToString(Utility.prepareInitialCentroids(args[0], Integer.parseInt(args[3])))
                );

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(centroidsFilePath));

        conf.setMapperClass(Mapper.class);
        conf.setCombinerClass(Combiner.class);
        conf.setReducerClass(Reducer.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        JobClient.runJob(conf).waitForCompletion();

        // the same file should now be updated with the new centroids.
        return Utility.loadCentroidsFromFile(centroidsFilePath);
    }

    public static void runDataLabellingJob(String[] args, List<List<Double>> centroids) throws IOException {
        JobConf conf = new JobConf(Driver.class);

        conf.setJobName("KMeans_MapReduce_Points_Labelling");
        conf.set("centroids", Utility.convertCentroidsToString(centroids));

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        // using the same mapper of the centroids computation job.
        conf.setMapperClass(Mapper.class);
        conf.setReducerClass(org.ds.kmeans.labelling.Reducer.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        boolean jobWasSuccessful = JobClient.runJob(conf).isSuccessful();
        if (!jobWasSuccessful) {
            throw new RuntimeException("Job failed.");
        }
    }

}