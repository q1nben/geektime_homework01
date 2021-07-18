package me.qinben;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StreamCountApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 生成job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置job属性
        job.setJobName("qinben");                           // 设置作业名称
        job.setJarByClass(StreamCountApp.class);            // 设置jar包搜索类
        job.setInputFormatClass(TextInputFormat.class);     // 设置输入格式
        job.setOutputFormatClass(TextOutputFormat.class);   // 设置输出格式

        job.setMapperClass(StreamMapper.class);             // 设置mapper class
        job.setReducerClass(StreamReducer.class);           // 设置reducer class

        job.setNumReduceTasks(1);                           // 设置reducer个数
        job.setMapOutputKeyClass(Text.class);               // 设置mapper输出 key类型
        job.setMapOutputValueClass(StreamBean.class);       // 设置mapper输出 value类型
        job.setOutputKeyClass(Text.class);                  // 设置reducer输出 key类型
        job.setOutputValueClass(StreamBean.class);          // 设置reducer输出 value类型

        // 指定处理数据路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
