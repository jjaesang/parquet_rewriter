package com.github.henry.mapreduce.parquet;

import com.github.henry.convert.NullValueConveter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ParquetRewriterMapper extends Mapper<LongWritable, Group, Void, Group> {

    private NullValueConveter nullValueConveter;
    private Set<String> targetColNameSet;

    private enum Statistics {
        INPUT, OUTPUT, TRANSFROM
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        MessageType schema = GroupWriteSupport.getSchema(conf);
        targetColNameSet = new HashSet<>();
        String keyColName = conf.get("");

        nullValueConveter = new NullValueConveter(schema, targetColNameSet, keyColName);

    }

    @Override
    protected void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {

        context.write(null, nullValueConveter.convertGroup(value));

        context.getCounter(Statistics.INPUT).increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
