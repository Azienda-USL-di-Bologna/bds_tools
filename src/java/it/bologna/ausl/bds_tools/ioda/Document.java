package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import java.io.IOException;
import java.io.InputStream;
import java.util.TimeZone;


public abstract class Document {
    public static enum DocumentOperationType {
        INSERT("insert"),
        UPDATE("update"),
        DELETE("delete");
        private final String key;

        DocumentOperationType(String key) {
            this.key = key;
        }

        @JsonCreator
        public static DocumentOperationType fromString(String key) {
            return key == null
                    ? null
                    : DocumentOperationType.valueOf(key.toUpperCase());
        }

        @JsonValue
        public String getKey() {
            return key;
        }
    }
    
    protected String idOggettoOrigine; // obbligatorio, id dell'oggetto nel sistema di chi sta versando
    protected String tipoOggettoOrigine; // obbligatorio, tipo dell'oggetto nel sistema di chi sta versando

    public String getIdOggettoOrigine() {
        return idOggettoOrigine;
    }

    public void setIdOggettoOrigine(String idOggettoOrigine) {
        this.idOggettoOrigine = idOggettoOrigine;
    }

    public String getTipoOggettoOrigine() {
        return tipoOggettoOrigine;
    }

    public void setTipoOggettoOrigine(String tipoOggettoOrigine) {
        this.tipoOggettoOrigine = tipoOggettoOrigine;
    }

    @JsonIgnore
    public static <T extends Document>T parse(String value, Class<T> documentClass) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.setTimeZone(TimeZone.getDefault());
        return mapper.readValue(value, documentClass);
    }
    
    @JsonIgnore
    public static <T extends Document>T parse(InputStream value, Class<T> documentClass) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.setTimeZone(TimeZone.getDefault());
        return mapper.readValue(value, documentClass);
    }
    
    @JsonIgnore
    public String getJSONString() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.setTimeZone(TimeZone.getDefault());
        String writeValueAsString = mapper.writeValueAsString(this);
        return writeValueAsString;
    }
}
