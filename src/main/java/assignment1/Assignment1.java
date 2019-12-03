package assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.TreeSet;

/**
 * This class solves the problem posed for Assignment1
 */
public class Assignment1 {

    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        // set 'ngram' and 'minimum count' parameter for mapper and reducer
        conf.setInt("ngram", Integer.valueOf(args[0]));
        conf.setInt("minimumCount", Integer.valueOf(args[1]));
        
        Job job = Job.getInstance(conf, "assignment 1");
        job.setJarByClass(Assignment1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        // tokens as key
        private Text words = new Text();
        // fileName as value
        private Text fileName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            // Get the ngram from configuration
            int ngram = Integer.valueOf(context.getConfiguration().get("ngram"));
            // Split content of file with space.
            String[] tokens = value.toString().split(" ");
            // The position where traversal end.
            int tokenEnd = tokens.length + 1 - ngram;
            for (int i = 0; i < tokenEnd; i++) {
                int j = 0;
                String[] tokensArray = new String[ngram];
                // get the N tokens referencing ngram.
                while (j < ngram) {
                    tokensArray[j] = tokens[i + j];
                    j++;
                }
                words.set(String.join(" ", tokensArray));
                // Get the filename that the map is reading from
                fileName.set(((FileSplit) context.getInputSplit()).getPath().getName());
                context.write(words, fileName);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int minimumCount = Integer.valueOf(context.getConfiguration().get("minimumCount"));
            // New a TreeSet object for remove duplicate filename.
            TreeSet<String> fileSet = new TreeSet<>();
            // Count the number of times token appear.
            for (Text val : values) {
                fileSet.add(val.toString());
                sum++;
            }
            // Construct the result with count and filename.
            result.set(sum + "  " + String.join(" ", fileSet));
            // Output the result if the number reach the minimum count.
            if (sum >= minimumCount) context.write(key, result);
        }
    }

}