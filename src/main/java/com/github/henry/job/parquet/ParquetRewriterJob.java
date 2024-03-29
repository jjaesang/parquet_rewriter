package com.github.henry.job.parquet;

import com.github.henry.job.RunnableHdfsJob;
import com.github.henry.mapreduce.parquet.ParquetRewriterMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ParquetRewriterJob extends RunnableHdfsJob {

    public ParquetRewriterJob(Configuration configuration) {
        super(configuration);
    }

    @Override
    public JobReturnCode runJob(String jobName, String[] inputs, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();
        int numberOfReduceTasks = 0;

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ParquetRewriterJob.class);

        for (final String input : inputs) FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        try {
            job.addCacheFile(new URI("hdfs://localhost:9000/cached_Geeks/stopWords.txt"));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        job.setMapperClass(ParquetRewriterMapper.class);
        job.setMapOutputKeyClass(Void.class);
        job.setMapOutputValueClass(Group.class);

        job.setInputFormatClass(ParquetInputFormat.class);
        job.setOutputFormatClass(ParquetOutputFormat.class);

        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);

        job.setNumReduceTasks(numberOfReduceTasks);

        return job.waitForCompletion(true) ? JobReturnCode.SUCCESS : JobReturnCode.FAILURE;

    }
}
