package edu.indiana.d2i.hadoop;

import edu.indiana.d2i.hadoop.custom.ProvValue;
import edu.indiana.d2i.prov.streaming.ProvFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class HashCountProvFile {

    public static class HashFinderMapper
            extends Mapper<Object, Text, Text, ProvValue> {

        private final static IntWritable one = new IntWritable(1);
        private Text hashTag = new Text();
        private int count;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                String filename = fileSplit.getPath().getName();
                String inputId = filename + "_" + key.toString();
                String invocationId = context.getTaskAttemptID().getTaskID().toString() + "_" + inputId;
                // capture provenance
                ProvFileWriter kafkaProducer = ProvFileWriter.getInstance();
                int partition = ProvFileWriter.getPartitionToWrite();
                if (partition < 0)
                    if ("horizontal".equals(ProvFileWriter.getPartitionStrategy()))
                        partition = 0;
                    else
                        partition = count++ % ProvFileWriter.getNumberOfPartitions();
                kafkaProducer.createAndSendEdge(invocationId, inputId, "used", partition);

                StringTokenizer itr = new StringTokenizer(value.toString());
                int outCount = 0;
                List<String> nots = new ArrayList<>();
                while (itr.hasMoreTokens()) {
                    String token = itr.nextToken();
                    if (token.startsWith("#")) {
                        if (token.contains("\""))
                            token = token.replace("\"", "");
                        if (token.contains("\\"))
                            token = token.replace("\\", "");
                        hashTag.set(token.toLowerCase());
                        String outputId = inputId + "_out" + outCount++;
                        nots.add(kafkaProducer.createEdge(outputId, invocationId, "wasGeneratedBy", partition));
                        context.write(hashTag, new ProvValue(one, new Text(outputId)));
                    }
                }
                kafkaProducer.createAndSendJSONArray(nots, "wasGeneratedBy", partition);
            } catch (Exception e) {
                System.out.println("ERROR: " + value);
                // ignore
            }
        }

    }

    public static class IntSumReducer
            extends Reducer<Text, ProvValue, Text, ProvValue> {

        private IntWritable result = new IntWritable();
        private int count;

        public void reduce(Text key, Iterable<ProvValue> values,
                           Context context) throws IOException, InterruptedException {
            String reduceId = context.getTaskAttemptID().toString() + "_" + key;

            int partition = ProvFileWriter.getPartitionToWrite();
            if (partition < 0)
                if ("horizontal".equals(ProvFileWriter.getPartitionStrategy()))
                    partition = reduceId.contains("_m_") ? 1 : 2;
                else
                    partition = count++ % ProvFileWriter.getNumberOfPartitions();

            List<String> nots = new ArrayList<>();
            ProvFileWriter kafkaProducer = ProvFileWriter.getInstance();
            int sum = 0;
            for (ProvValue val : values) {
                sum += val.getSum().get();
                String inputId = val.getDataId().toString();
                nots.add(kafkaProducer.createEdge(reduceId, inputId, "used", partition));
            }
            kafkaProducer.createAndSendJSONArray(nots, "used", partition);
            result.set(sum);
            String reduceOutId = reduceId + "_out";
            kafkaProducer.createAndSendEdge(reduceOutId, reduceId, "wasGeneratedBy", partition);
            context.write(key, new ProvValue(result, new Text(reduceOutId)));
        }

    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        ProvFileWriter.getInstance();
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://hadoop-master2:9000");
//        conf.set("mapreduce.job.tracker", "hadoop-master2:5431");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.address", "hadoop-master2:8050");
        Job job = Job.getInstance(conf, "hashtag count - full prov kafka");
        job.setJarByClass(HashCountProvKafka.class);
        job.setMapperClass(HashFinderMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ProvValue.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean complete = job.waitForCompletion(true);
        ProvFileWriter.getInstance().close();
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("++++++++++ Job complete time(ms): " + totalTime);
        System.exit(complete ? 0 : 1);
    }

}

