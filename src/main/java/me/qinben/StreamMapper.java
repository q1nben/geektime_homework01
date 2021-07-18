package me.qinben;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class StreamMapper extends Mapper<LongWritable, Text, Text, StreamBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取行内容
        String line = value.toString();
        // 切分字段
        String[] fields = line.split("\t");
        // 获取手机号
        String phoneNumber = fields[1];
        // 获取上行流量
        long upstream = Long.parseLong(fields[fields.length-3]);
        // 获取下行流量
        long downstream = Long.parseLong(fields[fields.length-2]);
        // 流量统计Bean
        StreamBean bean = new StreamBean();
        bean.setStreamData(upstream, downstream);
        // keyText设置
        Text keyText = new Text();
        keyText.set(phoneNumber);

        // 写入键值
        context.write(keyText, bean);
    }
}