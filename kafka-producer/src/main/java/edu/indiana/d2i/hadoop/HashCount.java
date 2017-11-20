package edu.indiana.d2i.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class HashCount {

    public static class HashFinderMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text hashTag = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    String token = itr.nextToken();
                    if (token.startsWith("#")) {
                        hashTag.set(token.toLowerCase());
                        context.write(hashTag, one);
                    }
                }
            } catch (Exception e) {
                System.out.println("ERROR: " + value);
                // ignore
            }
        }

    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://hadoop-master2:9000");
//        conf.set("mapreduce.job.tracker", "hadoop-master2:5431");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.address", "hadoop-master2:8050");
        Job job = Job.getInstance(conf, "hashtag count - original");
        job.setJarByClass(HashCount.class);
        job.setMapperClass(HashFinderMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean complete = job.waitForCompletion(true);
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("++++++++++ Job complete time(ms): " + totalTime);
        System.exit(complete ? 0 : 1);
    }

}

