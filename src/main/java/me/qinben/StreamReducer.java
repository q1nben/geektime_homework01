package me.qinben;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StreamReducer extends Reducer<Text, StreamBean, Text, StreamBean> {

    @Override
    protected void reduce(Text key, Iterable<StreamBean> values, Context context) throws IOException, InterruptedException {

        // 总上行流量
        long totalUpstream = 0;
        // 总下行流量
        long totalDownstream = 0;
        // 统计流量
        for (StreamBean value : values) {
            totalUpstream += value.getUpstream();
            totalDownstream += value.getDownstream();
        }

        StreamBean bean = new StreamBean();
        bean.setStreamData(totalUpstream, totalDownstream);

        context.write(key, bean);
    }
}
