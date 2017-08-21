package edu.indiana.d2i.hadoop;


import edu.indiana.d2i.prov.streaming.ProvKafkaProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import edu.indiana.d2i.hadoop.custom.ProvValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class WordCountProvKafka {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, ProvValue> {

        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String inputId = filename + "_" + key.toString();
            String invocationId = context.getTaskAttemptID().getTaskID().toString() + "_" + inputId;

//            ProvKafkaProducer.getInstance().createEntity(inputId);
//            ProvKafkaProducer.getInstance().createActivity(invocationId, "map");
            ProvKafkaProducer kafkaProducer = ProvKafkaProducer.getInstance();
            kafkaProducer.createAndSendEdge(invocationId, inputId, "used");

            StringTokenizer itr = new StringTokenizer(value.toString());
            int outCount = 0;
            List<String> nots = new ArrayList<>();
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                word.set(token);
                String outputId = inputId + "_out" + outCount++;
                Text outId = new Text(outputId);
//                ProvKafkaProducer.getInstance().createEntity(outputId, token, "1");
                nots.add(kafkaProducer.createEdge(outputId, invocationId, "wasGeneratedBy"));
                context.write(word, new ProvValue(one, outId));
            }
            kafkaProducer.createAndSendJSONArray(nots, "wasGeneratedBy");
        }

    }

    public static class IntSumReducer extends Reducer<Text, ProvValue, Text, ProvValue> {

        public void reduce(Text key, Iterable<ProvValue> values,
                           Context context) throws IOException, InterruptedException {
            String reduceId = context.getTaskAttemptID().toString() + "_" + key;
//            ProvKafkaProducer.getInstance().createActivity(reduceId, "reduce");
            int sum = 0;
            List<String> nots = new ArrayList<>();
            ProvKafkaProducer kafkaProducer = ProvKafkaProducer.getInstance();
            while (values.iterator().hasNext()) {
                ProvValue val = values.iterator().next();
                sum += val.getSum().get();
                String inputId = val.getDataId().toString();
                nots.add(kafkaProducer.createEdge(reduceId, inputId, "used"));
            }
            kafkaProducer.createAndSendJSONArray(nots, "used");

            String reduceOutId = reduceId + "_out";
//            ProvKafkaProducer.getInstance().createEntity(reduceOutId, key.toString(), "" + sum);
            kafkaProducer.createAndSendEdge(reduceOutId, reduceId, "wasGeneratedBy");
            context.write(key, new ProvValue(new IntWritable(sum), new Text(reduceOutId)));
        }

    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        ProvKafkaProducer.getInstance();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ProvValue.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean complete = job.waitForCompletion(true);
//        ProvKafkaProducer.getInstance().flush();
        ProvKafkaProducer.getInstance().close();
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("++++++++++ Job complete time(ms): " + totalTime);
        System.exit(complete ? 0 : 1);
    }
    
}
