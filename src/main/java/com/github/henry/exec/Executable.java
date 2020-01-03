package com.github.henry.exec;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public abstract class Executable extends Configured implements Tool {

    /**
     * Result code of execution.
     */
    public enum ExecuteReturnCode {
        SUCCESS(0),
        FAILURE(1);

        private int code;

        ExecuteReturnCode(int code) {
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
     * Create new instance with {@link org.apache.hadoop.conf.Configuration}.
     *
     * @param conf
     */
    public Executable(Configuration conf) {
        super(conf);
    }

    /**
     * Prints Executable command-line arguments and usage information with {@link java.lang.System#exit(int)}.
     *
     * @param className
     */
    public void printHelpWithExit(String className) {
        new HelpFormatter().printHelp(className, getOptions());
        System.exit(1);
    }

    /**
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public abstract int run(String[] args) throws Exception;

    /**
     * Get Executable Options.
     *
     * @return Options
     */
    public abstract Options getOptions();

    /**
     * Must option check.
     *
     * @param cl
     * @return boolean
     */
    public abstract boolean mustOptions(CommandLine cl);

}
