/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import java.io.IOException;

/**
 *
 * @author utente
 */
public class Prova {
    private String a;
    enum mio{
    TIPO1("tipo1"), 
    TIPO2("tipo2");

    private String key;

    mio(String key) {
        this.key = key;
    }

    @JsonCreator
    public static mio fromString(String key) {
        return key == null
                ? null
                : mio.valueOf(key.toUpperCase());
    }

    @JsonValue
    public String getKey() {
        return key;
    }
    }
    private mio m;
    
    public Prova() {
    }

    public Prova(String a, mio m) {
        this.a = a;
        this.m = m;
    }

    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    public mio getM() {
        return m;
    }

    public void setM(mio m) {
        this.m = m;
    }
    

    public static Prova parse(String value) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        return mapper.readValue(value, Prova.class);
    }
    
    @JsonIgnore
    public String getJSONString() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        String writeValueAsString = mapper.writeValueAsString(this);
        return writeValueAsString;
    }
}
