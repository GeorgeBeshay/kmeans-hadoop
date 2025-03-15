package org.ds.kmeans;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

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
     *             - Feature vector size
     * @throws IOException Problem had occurred while reading an input file during the job.
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 5) {
            System.err.println(
                    "Usage: KMeansParallelDriver" +
                    "\n\t<data_input_filepath>" +
                    "\n\t<centroids_output_filepath>" +
                    "\n\t<data_labels_output_filepath>" +
                    "\n\t<num_centroids>" +
                    "\n\t<feature_vec_size>"
            );
            System.exit(1);
        }

        int currentIteration = 0;
        boolean converged = false;
        boolean isInitial = true;

        while (currentIteration < MAX_ITERATIONS && !converged) {
            converged = runCentroidsComputationJob(args, isInitial);
            currentIteration++;
            isInitial = false;
        }

        runDataLabellingJob(args);
    }

    public static boolean runCentroidsComputationJob(String[] args, boolean isInitialJob) throws IOException {
        List<List<Double>> currentCentroids, newCentroids;

        JobConf conf = new JobConf(Driver.class);
        conf.setJobName("KMeans_MapReduce_Centroids_Computation");
        conf.set("centroidsFp", args[1]);
        conf.setBoolean("isInitialJob", isInitialJob);
        conf.setInt("numberOfCentroids", Integer.parseInt(args[3]));
        conf.setInt("featureVectorSize", Integer.parseInt(args[4]));

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(org.ds.kmeans.centroids.Mapper.class);
        conf.setCombinerClass(org.ds.kmeans.centroids.Combiner.class);
        conf.setReducerClass(org.ds.kmeans.centroids.Reducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        currentCentroids = Utility.loadCentroidsFromFile(conf);
        boolean jobWasSuccessful = JobClient.runJob(conf).isSuccessful();
        if (!jobWasSuccessful) {
            throw new RuntimeException("Job failed.");
        }
        newCentroids = Utility.loadCentroidsFromFile(conf);
        // the same file should now be updated with the new centroids.

        return Utility.isConverged(currentCentroids, newCentroids, CONVERGENCE_THRESHOLD);
    }

    public static void runDataLabellingJob(String[] args) throws IOException {
        JobConf conf = new JobConf(Driver.class);

        conf.setJobName("KMeans_MapReduce_Points_Labelling");
        conf.set("centroidsFp", args[1]);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        // using the same mapper of the centroids computation job.
        conf.setMapperClass(org.ds.kmeans.centroids.Mapper.class);
        conf.setReducerClass(org.ds.kmeans.labelling.Reducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        boolean jobWasSuccessful = JobClient.runJob(conf).isSuccessful();
        if (!jobWasSuccessful) {
            throw new RuntimeException("Job failed.");
        }
    }

}