package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import it.bologna.ausl.bds_tools.ioda.utils.exceptions.IodaDocumentException;
import it.bologna.ausl.bds_tools.ioda.utils.exceptions.IodaFileException;
import java.io.IOException;
import java.io.InputStream;
import java.util.TimeZone;

/**
 *
 * @author gdm
 */
public class IodaRequest {
    private String idApplicazione;    
    private String tokenApplicazione;
    private GdDoc document;

    public IodaRequest() {
    }

    public IodaRequest(String idApplicazione, String tokenApplicazione, GdDoc doc) {
        this.idApplicazione = idApplicazione;
        this.tokenApplicazione = tokenApplicazione;
        this.document = doc;
    }

    public String getIdApplicazione() {
        return idApplicazione;
    }

    public void setIdApplicazione(String idApplicazione) {
        this.idApplicazione = idApplicazione;
    }

    public String getTokenApplicazione() {
        return tokenApplicazione;
    }

    public void setTokenApplicazione(String tokenApplicazione) {
        this.tokenApplicazione = tokenApplicazione;
    }

    public GdDoc getDocument() {
        return document;
    }

    public void setDocument(GdDoc doc) {
        this.document = doc;
    }
    
    @JsonIgnore
    public static IodaRequest parse(String value) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.setTimeZone(TimeZone.getDefault());
        return mapper.readValue(value, IodaRequest.class);
    }
    
    @JsonIgnore
    public static IodaRequest parse(InputStream value) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.setTimeZone(TimeZone.getDefault());
        return mapper.readValue(value, IodaRequest.class);
    }
    
    @JsonIgnore
    public String getJSONString() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.setTimeZone(TimeZone.getDefault());
        String writeValueAsString = mapper.writeValueAsString(this);
        return writeValueAsString;
    }
    
    @JsonIgnore
    public GdDoc getGdDoc(Document.DocumentOperationType operationType) throws IodaDocumentException, IOException, IodaFileException{
        GdDoc gdDoc = getDocument();
        if (gdDoc == null) {
            throw new IodaDocumentException("json descrittivo del documento da inserire mancante");
        }
        gdDoc.check(operationType);

        return gdDoc;
    }
}
