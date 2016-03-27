package com.njp.learn.hadoop_learn;


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    
    // �ύ����Ⱥ������ȥ,���ڰ汾�г�ͻ���ڼ�Ⱥ�����»ᱨ��
/*    conf.set("fs.default.name", "hdfs://192.168.1.237:9000");   
    conf.set("hadoop.job.user","hadoop");    
    conf.set("mapreduce.framework.name","yarn"); 
//    conf.set("mapred.job.tracker","192.168.1.187:9001"); ����������ö��ø����ã��������Ǿɰ汾�����ã��Լ��õ���hadoop2.3.0���鿴�ٷ������ĵ����������õ�������mapreduce.jobtracker.address�����õ�ַ
    conf.set("mapreduce.jobtracker.address","192.168.1.237:9001"); 
     conf.set("yarn.resourcemanager.hostname", "192.168.1.237");
     conf.set("yarn.resourcemanager.admin.address", "192.168.1.237:8033");
     conf.set("yarn.resourcemanager.address", "192.168.1.237:8032");
     conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.1.237:8036");
     conf.set("yarn.resourcemanager.scheduler.address", "192.168.1.237:8030");*/
    
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    // ɾ�����Ŀ¼����ֹJOBʧ��
    FileSystem hdfs = FileSystem.get(conf);
    Path path = new Path(otherArgs[otherArgs.length - 1]);
    boolean isDeleted = hdfs.delete(path);
    
    hdfs.close();
    
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
