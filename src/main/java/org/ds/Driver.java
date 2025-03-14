package org.ds;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Driver {

    static int MAX_ITERATIONS = 10;

    public static void main(String[] args) throws IOException {

        int currentIteration = 0;
        boolean converged = false;

        while (currentIteration < MAX_ITERATIONS && !converged) {
            JobConf conf = new JobConf(Driver.class);
            conf.setJobName("KMeans_MapReduce");
            conf.set("centroidsFp", args[2]);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

            conf.setMapperClass(Map.class);
            conf.setCombinerClass(Combiner.class);
            conf.setReducerClass(Reducer.class);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            JobClient.runJob(conf);

            // TODO: Implement the logic to compare the current and previous iterations centroids.
        }
    }
}