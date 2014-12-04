/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.jobs.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author andrea
 */
public class Test {

    public static void main(String[] args) throws JsonProcessingException, FileNotFoundException, IOException {
        JobParams jp = new JobParams();
        jp.addParam("k1", "v1");
        jp.addParam("k2", "v2");
        JobDescriptor jd = new JobDescriptor();
        jd.setJobParams(jp);
        jd.setSchedule("schedule1");
        jd.setName("job name");
        jd.setClassz("job class");
        JobList jl = new JobList();
        jl.addJob(jd);

        ObjectMapper mapper = new ObjectMapper();
        String writeValueAsString = mapper.writeValueAsString(jl);
        FileOutputStream out = new FileOutputStream("/tmp/joblist.json");
        out.write(writeValueAsString.getBytes("UTF8"));
        out.close();
        System.out.println(writeValueAsString);
        String json = IOUtils.toString(new FileReader("/tmp/joblist.json"));
        JobList jl2 = null;
        jl2 = mapper.readValue(json, JobList.class);
        //jl2 = mapper.readValue("/tmp/joblist.json", JobList.class);
        System.out.println(jl2.getJobs().get(0).getJobParams().getParam("k1"));

    }
}
