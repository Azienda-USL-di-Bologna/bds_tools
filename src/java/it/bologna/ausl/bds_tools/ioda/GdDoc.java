package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import it.bologna.ausl.bds_tools.ioda.utils.exceptions.IodaDocumentException;
import it.bologna.ausl.bds_tools.ioda.utils.exceptions.IodaFileException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;

/**
 *
 * @author utente
 */
public class GdDoc extends Document{
    private String nome; // obbligatorio, nella tabella nome_gddoc
    private boolean record = true; // opzionale, se non passato = true. Indica se è un record
    
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="yyyy-MM-dd'T'HH:mm:ssZ")
    private DateTime dataUltimaModifica; //opzionale, si riferisce alla modifica nei loro sistemi
    private boolean visibile = true; // opzionale, se non passato = true. indica se il gdDoc deve essere visibile
    private String codiceRegistro; // obbligatorio se record = true
    
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="yyyy-MM-dd'T'HH:mm:ssZ")
    private DateTime dataRegistrazione; // obbligatorio se record = true
    private String numeroRegistrazione; // obbligatorio se record = true
    
    private String xmlSpecificoParer; // opzionale per versamento parer (se non passato il documento non viene versato. Il versamento avviene poi nel momento in cui il parametro viene passato)
    private boolean forzaConservazione = false; // opzionale per versamento parer, se non passato = false
    private boolean forzaAccettazione = false; // opzionale per versamento parer, se non passato = false
    private boolean forzaCollegamento = false; // opzionale per versamento parer, se non passato = false

    private List<Fascicolo> fascicoli; // obbligatorio (almeno un elemento nella lista): fascicoli in cui il gddoc andrà inserito
    private List<SottoDocumento> sottoDocumenti; // opzionale: i sottodocumenti del GdDoc
    
    public GdDoc() {
    }
  
    public GdDoc(String idOggettoOrigine, String tipoOggettoOrigine, String nome, boolean record, DateTime dataUltimaModifica, boolean visibile, String codiceRegistro, DateTime dataRegistrazione, String numeroRegistrazione, String xmlSpecificoParer, boolean forzaConservazione, boolean forzaAccettazione, boolean forzaCollegamento) {
        super.idOggettoOrigine = idOggettoOrigine;
        super.tipoOggettoOrigine = tipoOggettoOrigine;
        this.nome = nome;
        this.record = record;
        this.dataUltimaModifica = dataUltimaModifica;
        this.visibile = visibile;
        this.codiceRegistro = codiceRegistro;
        this.dataRegistrazione = dataRegistrazione;
        this.numeroRegistrazione = numeroRegistrazione;
        this.xmlSpecificoParer = xmlSpecificoParer;
        this.forzaConservazione = forzaConservazione;
        this.forzaAccettazione = forzaAccettazione;
        this.forzaCollegamento = forzaCollegamento;
    }

    public GdDoc(String idOggettoOrigine, String tipoOggettoOrigine, String nome, boolean record, DateTime dataUltimaModifica, boolean visibile, String codiceRegistro, DateTime dataRegistrazione, String numeroRegistrazione) {
        super.idOggettoOrigine = idOggettoOrigine;
        super.tipoOggettoOrigine = tipoOggettoOrigine;
        this.nome = nome;
        this.record = record;
        this.dataUltimaModifica = dataUltimaModifica;
        this.visibile = visibile;
        this.codiceRegistro = codiceRegistro;
        this.dataRegistrazione = dataRegistrazione;
        this.numeroRegistrazione = numeroRegistrazione;
    }

    @JsonIgnore
    public static GdDoc getGdDoc(String gdDocJson, DocumentOperationType operationType) throws IodaDocumentException, IOException, IodaFileException{
        if (gdDocJson == null || gdDocJson.equals("")) {
            throw new IodaDocumentException("json descrittivo del documento da inserire mancante");
        }

        GdDoc gdDoc = GdDoc.parse(gdDocJson, GdDoc.class);
        gdDoc.check(operationType);

        return gdDoc;
    }
    
    @JsonIgnore
    public static GdDoc getGdDoc(InputStream gdDocIs, DocumentOperationType operationType) throws IodaDocumentException, IOException {
        if (gdDocIs == null) {
            throw new IodaDocumentException("json descrittivo del documento da inserire mancante");
        }

        GdDoc gdDoc = GdDoc.parse(gdDocIs, GdDoc.class);
        gdDoc.check(operationType);

        return gdDoc;
    }
    
    @JsonIgnore
    public void check(DocumentOperationType operationType) throws IodaDocumentException {
        if (getIdOggettoOrigine() == null || getIdOggettoOrigine().equals("")) {
            throw new IodaDocumentException("idOggettoOrigine mancante");
        }
        else if (getTipoOggettoOrigine() == null || getTipoOggettoOrigine().equals("")) {
            throw new IodaDocumentException("tipoOggettoOrigine mancante");
        }
        else if (operationType == DocumentOperationType.INSERT && (getNome() == null || getNome().equals(""))) {
            throw new IodaDocumentException("nome mancante");
        }
        else if (operationType != DocumentOperationType.DELETE && isRecord()) {
            if (getCodiceRegistro() == null || getCodiceRegistro().equals(""))
                throw new IodaDocumentException("codiceRegistro mancante. Questo campo è obbligatorio se il documento è un record");
            if (getNumeroRegistrazione() == null || getNumeroRegistrazione().equals(""))
                throw new IodaDocumentException("numeroRegistrazione mancante. Questo campo è obbligatorio se il documento è un record");
            if (getDataRegistrazione() == null)
                throw new IodaDocumentException("dataRegistrazione mancante. Questo campo è obbligatorio se il documento è un record");
        }
        else if (operationType == DocumentOperationType.INSERT && (fascicoli == null || fascicoli.isEmpty())) {
            throw new IodaDocumentException("fascicoli mancanti");
        }
        
        // fascicoli
        for (Fascicolo f : fascicoli) {
            f.check(operationType);
        }
        
        // sottodocumenti
        if (sottoDocumenti != null && sottoDocumenti.size() > 0) {
            // controllo sulla validità dei sottodocumenti
            for (SottoDocumento sd : sottoDocumenti) {
                sd.check(operationType);
            }
        }
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

    public String getCodiceRegistro() {
        return codiceRegistro;
    }

    public void setCodiceRegistro(String codiceRegistro) {
        this.codiceRegistro = codiceRegistro;
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

    public List<Fascicolo> getFascicoli() {
        return fascicoli;
    }

    public void setFascicoli(List<Fascicolo> fascicoli) {
        this.fascicoli = fascicoli;
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
