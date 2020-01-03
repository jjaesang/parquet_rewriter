package com.github.henry.mapreduce.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class ParquetRewriterMapper extends Mapper<LongWritable, Group, Void, Group> {

    private enum Statistics {
        INPUT, OUTPUT, TRANSFROM
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

    }

    @Override
    protected void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {

        context.getCounter(Statistics.INPUT).increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
