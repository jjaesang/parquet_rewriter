package com.github.henry.exec.parquet;

import com.github.henry.exec.Executable;
import com.github.henry.job.RunnableHdfsJob;
import com.github.henry.job.RunnableJob;
import com.github.henry.job.parquet.ParquetRewriterJob;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class ParquetRewriter extends Executable {

    private static final Log LOG = LogFactory.getLog(ParquetRewriter.class.getName());

    public ParquetRewriter(Configuration configuration){
        super(configuration);
    }

    @Override
    public int run(String[] args) throws Exception {

        CommandLine cl = new PosixParser().parse(getOptions(), args);
        if (!mustOptions(cl)) printHelpWithExit(this.getClass().getName());

        Configuration conf = getConf();

        String[] inputs = cl.getOptionValues("i");
        String output = cl.getOptionValue("o");

        RunnableHdfsJob runJob = new ParquetRewriterJob(conf);
        RunnableJob.JobReturnCode jobreturnCode = runJob.runJob("JobName", inputs, output);

        ExecuteReturnCode executeReturnCode = null;
        if (jobreturnCode == RunnableJob.JobReturnCode.SUCCESS) executeReturnCode = ExecuteReturnCode.SUCCESS;
        else executeReturnCode = ExecuteReturnCode.FAILURE;


        return executeReturnCode.code();


    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption("i", "input", true, "input directory\n"
                + "             multi-values: repeat the option '-i'.\n"
                + "             example: -i path1 -i path2 -i path3");
        options.addOption("o", "output", true, "output directory");
        options.addOption("r", "reducer", true, "number of reduce taskers");
        options.addOption("c", "config", true, "user config file name");
        return options;
    }

    @Override
    public boolean mustOptions(CommandLine cl) {
        if (cl == null) return false;
        if (!cl.hasOption("i")) return false;
        if (!cl.hasOption("o")) return false;
        if (!cl.hasOption("r")) return false;
        if (!cl.hasOption("c")) return false;
        return true;
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            int res = ToolRunner.run(conf, new ParquetRewriter(conf), args);
            System.exit(res);
        } catch (Exception e) {
            LOG.error("Exception", e);
            System.exit(1);
        }
    }
}
