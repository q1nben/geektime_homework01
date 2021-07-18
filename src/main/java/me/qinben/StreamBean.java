package me.qinben;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StreamBean implements Writable {

    // 上行流量
    private long upstream;
    // 下行流量
    private long downstream;
    // 总流量
    private long totalStream;
    public StreamBean() {

    }

    public StreamBean(long upstream, long downstream, long totalStream) {
        this.upstream = upstream;
        this.downstream = downstream;
        this.totalStream = totalStream;
    }

    public void setStreamData(long upstream, long downstream) {
        this.upstream = upstream;
        this.downstream = downstream;
        this.totalStream = this.upstream + this.downstream;
    }

    public long getUpstream() {
        return upstream;
    }

    public void setUpstream(long upstream) {
        this.upstream = upstream;
    }

    public long getDownstream() {
        return downstream;
    }

    public void setDownstream(long downstream) {
        this.downstream = downstream;
    }

    public long getTotalStream() {
        return totalStream;
    }

    public void setTotalStream(long totalStream) {
        this.totalStream = totalStream;
    }

    // 序列化处理
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.getUpstream());
        dataOutput.writeLong(this.getDownstream());
        dataOutput.writeLong(this.getTotalStream());
    }

    // 反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setUpstream(dataInput.readLong());
        setDownstream(dataInput.readLong());
        setTotalStream(dataInput.readLong());
    }

    @Override
    public String toString() {
        return getUpstream() + "\t" + getDownstream() + "\t" + getTotalStream();
    }
}
