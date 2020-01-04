package com.github.henry.mapreduce.parquet;

import com.github.henry.convert.NullValueConveter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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
        String keyColName = conf.get("");

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try {

                String line = "";
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());

                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                while ((line = reader.readLine()) != null) {
                    String[] words = line.split(" ");
                    for (int i = 0; i < words.length; i++) {
                        targetColNameSet.add(words[i]);
                    }
                }
            } catch (Exception e) {
                System.out.println("Unable to read the File");
                System.exit(1);
            }
        }
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
