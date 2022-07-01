/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.jobs.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

/**
 *
 * @author andrea
 */
public class JobDescriptor {

    private String name, schedule;
    private String role;
    private boolean active;
    private String classz;
    @JsonUnwrapped
    JobParams jobParams;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    @JsonProperty("class")
    public String getClassz() {
        return classz;
    }

    @JsonProperty("class")
    public void setClassz(String classz) {
        this.classz = classz;
    }

    public JobParams getJobParams() {
        return jobParams;
    }

    public void setJobParams(JobParams params) {
        this.jobParams = params;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }
    
    public boolean getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = Boolean.parseBoolean(active);
    }
}
