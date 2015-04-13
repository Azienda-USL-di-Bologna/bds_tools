package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.joda.time.DateTime;

/**
 *
 * @author utente
 */
public class GdDoc extends Document{
    private String idOggettoOrigine; // id dell'oggetto nel sistema di chi sta versando
    private String tipoOggettoOrigine; // tipo dell'oggetto nel sistema di chi sta versando
    private String nome; // nella tabella nome_gddoc
    private boolean record; // indica se è un record
    
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="yyyy-MM-dd'T'HH:mm:ssZ")
    private DateTime dataUltimaModifica; //si riferisce alla modifica nei loro sistemi
    private boolean visibile; // indica se il gdDoc deve essere visibile
    private String codice_registro; // opzionale, se è un record deve averlo
    
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="yyyy-MM-dd'T'HH:mm:ssZ")
    private DateTime dataRegistrazione; // obbligatorio se è record
    private String numeroRegistrazione; // obbligatorio se è record
    
    private String xmlSpecificoParer; // opzionale per versamento parer
    private boolean forzaConservazione; // opzionale per versamento parer
    private boolean forzaAccettazione; // opzionale per versamento parer
    private boolean forzaCollegamento; // opzionale per versamento parer

    private List<SottoDocumento> sottoDocumenti;
    
    public GdDoc() {
    }

        
    public GdDoc(String idOggettoOrigine, String tipoOggettoOrigine, String nome, boolean record, DateTime dataUltimaModifica, boolean visibile, String codice_registro, DateTime dataRegistrazione, String numeroRegistrazione, String xmlSpecificoParer, boolean forzaConservazione, boolean forzaAccettazione, boolean forzaCollegamento) {
        this.idOggettoOrigine = idOggettoOrigine;
        this.tipoOggettoOrigine = tipoOggettoOrigine;
        this.nome = nome;
        this.record = record;
        this.dataUltimaModifica = dataUltimaModifica;
        this.visibile = visibile;
        this.codice_registro = codice_registro;
        this.dataRegistrazione = dataRegistrazione;
        this.numeroRegistrazione = numeroRegistrazione;
        this.xmlSpecificoParer = xmlSpecificoParer;
        this.forzaConservazione = forzaConservazione;
        this.forzaAccettazione = forzaAccettazione;
        this.forzaCollegamento = forzaCollegamento;
    }

    public GdDoc(String idOggettoOrigine, String tipoOggettoOrigine, String nome, boolean record, DateTime dataUltimaModifica, boolean visibile, String codice_registro, DateTime dataRegistrazione, String numeroRegistrazione) {
        this.idOggettoOrigine = idOggettoOrigine;
        this.tipoOggettoOrigine = tipoOggettoOrigine;
        this.nome = nome;
        this.record = record;
        this.dataUltimaModifica = dataUltimaModifica;
        this.visibile = visibile;
        this.codice_registro = codice_registro;
        this.dataRegistrazione = dataRegistrazione;
        this.numeroRegistrazione = numeroRegistrazione;
    }

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

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public boolean isRecord() {
        return record;
    }

    public void setRecord(boolean record) {
        this.record = record;
    }

    public DateTime getDataUltimaModifica() {
        return dataUltimaModifica;
    }

    public void setDataUltimaModifica(DateTime dataUltimaModifica) {
        this.dataUltimaModifica = dataUltimaModifica;
    }

    public boolean isVisibile() {
        return visibile;
    }

    public void setVisibile(boolean visibile) {
        this.visibile = visibile;
    }

    public String getCodice_registro() {
        return codice_registro;
    }

    public void setCodice_registro(String codice_registro) {
        this.codice_registro = codice_registro;
    }

    public DateTime getDataRegistrazione() {
        return dataRegistrazione;
    }

    public void setDataRegistrazione(DateTime dataRegistrazione) {
        this.dataRegistrazione = dataRegistrazione;
    }

    public String getNumeroRegistrazione() {
        return numeroRegistrazione;
    }

    public void setNumeroRegistrazione(String numeroRegistrazione) {
        this.numeroRegistrazione = numeroRegistrazione;
    }

    public String getXmlSpecificoParer() {
        return xmlSpecificoParer;
    }

    public void setXmlSpecificoParer(String xmlSpecificoParer) {
        this.xmlSpecificoParer = xmlSpecificoParer;
    }

    public boolean isForzaConservazione() {
        return forzaConservazione;
    }

    public void setForzaConservazione(boolean forzaConservazione) {
        this.forzaConservazione = forzaConservazione;
    }

    public boolean isForzaAccettazione() {
        return forzaAccettazione;
    }

    public void setForzaAccettazione(boolean forzaAccettazione) {
        this.forzaAccettazione = forzaAccettazione;
    }

    public boolean isForzaCollegamento() {
        return forzaCollegamento;
    }

    public void setForzaCollegamento(boolean forzaCollegamento) {
        this.forzaCollegamento = forzaCollegamento;
    }

    public List<SottoDocumento> getSottoDocumenti() {
        return sottoDocumenti;
    }

    public void setSottoDocumenti(List<SottoDocumento> sottoDocumenti) {
        this.sottoDocumenti = sottoDocumenti;
    }
    
    @JsonIgnore
    public void addSottoDocumento(SottoDocumento sd){
        if(this.sottoDocumenti == null){
            this.sottoDocumenti = new ArrayList<SottoDocumento>();
        }
        this.sottoDocumenti.add(sd);
    }
}
