/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.jobs.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author andrea
 */
public class JobParams {

    @JsonProperty
    private final Map<String, String> params;

    public JobParams() {
        params = new HashMap<String, String>();
    }

    @JsonIgnore
    public Iterable<String> getParamNames() {
        return params.keySet();
    }

    public String getParam(String key) {
        return params.get(key);
    }

    public void addParam(String key, String value) {
        params.put(key, value);
    }
}
