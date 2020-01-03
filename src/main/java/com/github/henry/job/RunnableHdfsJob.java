package com.github.henry.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Counters;

import java.io.IOException;

public abstract class RunnableHdfsJob extends Configured implements RunnableJob {

    protected Counters counters = null;

    /**
     * Create new instance with {@link org.apache.hadoop.conf.Configuration}
     *
     * @param conf
     */
    public RunnableHdfsJob(Configuration conf) {
        super(conf);
    }

    /**
     * Make a {@link org.apache.hadoop.mapreduce.Job} with Map Reduce on HDFS.
     *
     * @param jobName
     * @param inputs
     * @param output
     * @return ReturnJobCode Zero if the job succeeded
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public abstract JobReturnCode runJob(String jobName, String[] inputs, String output) throws IOException, InterruptedException, ClassNotFoundException;


    @Override
    public Counters getCounters() {
        return counters;
    }

}