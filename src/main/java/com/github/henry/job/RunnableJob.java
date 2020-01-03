package com.github.henry.job;

import org.apache.hadoop.mapreduce.Counters;

public interface RunnableJob {

    /**
     * Result code of M/R job.
     */
    enum JobReturnCode {
        SUCCESS(0),
        FAILURE(1);

        private int code;

        JobReturnCode(int code) {
            this.code = code;
        }

        /**
         * Return code of this object.
         *
         * @return int
         */
        public int code() {
            return code;
        }
    }

    /**
     * Gets a {@link org.apache.hadoop.mapreduce.Counters} in a job.
     *
     * @return Counters
     */
    Counters getCounters();

}
