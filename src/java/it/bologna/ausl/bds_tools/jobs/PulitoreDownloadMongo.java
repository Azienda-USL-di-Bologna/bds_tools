package it.bologna.ausl.bds_tools.jobs;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */
public class PulitoreDownloadMongo implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        System.err.println("Ho Mongopulito ERR");
        System.out.println("Ho Mongopulito OUT");
    }

}
