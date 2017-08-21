package edu.indiana.d2i.hadoop.custom;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProvValue implements Writable {

    private IntWritable sum;
    private Text dataId;


    public ProvValue() {
        this.sum = new IntWritable();
        this.dataId = new Text();
    }

    public ProvValue(IntWritable val, Text id) {
        this.sum = val;
        this.dataId = id;
    }

    public void write(DataOutput dataOutput) throws IOException {
        sum.write(dataOutput);
        dataId.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        sum.readFields(dataInput);
        dataId.readFields(dataInput);
    }

    public IntWritable getSum() {
        return sum;
    }

    public Text getDataId() {
        return dataId;
    }

    public void setSum(IntWritable sum) {
        this.sum = sum;
    }

    public void setDataId(Text dataId) {
        this.dataId = dataId;
    }

    @Override
    public int hashCode() {
        // This is used by HashPartitioner, so implement it as per need
        // this one shall hash based on request id
        return sum.hashCode();
    }

    public String toString() {
        return sum.toString() + ":%%:" + dataId.toString();
    }
}

