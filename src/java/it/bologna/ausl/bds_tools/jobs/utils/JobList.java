/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.jobs.utils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author andrea
 */
public class JobList {

    private List<JobDescriptor> jobs;

    public JobList() {
        jobs = new ArrayList<JobDescriptor>();
    }

    public List<JobDescriptor> getJobs() {
        return jobs;
    }

    public void setJobs(ArrayList<JobDescriptor> jobs) {
        this.jobs = jobs;
    }

    public void addJob(JobDescriptor j) {
        jobs.add(j);
    }
}
