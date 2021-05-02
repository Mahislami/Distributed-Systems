import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Task3 {
	
  public static class EdgeWeightMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();
    private Text word2 = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        word2.set(itr.nextToken());
        int weight = Integer.parseInt(itr.nextToken());
        context.write(word, new IntWritable(-weight));
	context.write(word2, new IntWritable(weight));
      }
    }
  }
  public static class EdgeWeightReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if ( sum % 2 != 0 ) {
       result.set(sum);
       context.write(key, result);
      }
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", "\t");
    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);
    job.setMapperClass(EdgeWeightMapper.class);
    job.setCombinerClass(EdgeWeightReducer.class);
    job.setReducerClass(EdgeWeightReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
