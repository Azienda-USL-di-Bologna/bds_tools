package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import it.bologna.ausl.bds_tools.ioda.utils.exceptions.IodaDocumentException;
import java.util.List;

/**
 *
 * @author utente
 */
public class SottoDocumento extends Document{

    public static enum TipoFirma {
        GRAFOMETRICA("grafometrica"),
        DIGITALE("digitale"),
        AUTOGRAFA("autografa");

        private final String key;

        TipoFirma(String key) {
            this.key = key;
        }

        @JsonCreator
        public static TipoFirma fromString(String key) {
            return key == null
                    ? null
                    : TipoFirma.valueOf(key.toUpperCase());
        }

        @JsonValue
        public String getKey() {
            return key;
        }
    }

    private DocumentOperationType tipoOperazione; // obbligatorio: insert/update o delete. In caso di inserimento del GdDoc tutti i Sottodocumenti devono essere insert
    private String codiceSottoDocumento; // obbligatorio: codice che identifica univocamente il SottoDocumento
    private String tipo; // obbligatorio: tipo del sotto documento. Il tipo deve essere concordato con noi. Come con i registri
    private TipoFirma tipoFirma; // opzionale (obbligatorio se firmatari.length() > 0) : tipologia di firma: grafometrica, digitale, autografa
    private List<String> firmatari; // opzionale
    private Boolean principale; // obbligatorio: indica se il sottodocumento è da considerarsi come principale del GdDoc
    private String nome; // obbligatorio: nome del SottoDocumento
    private String uuidMongoPdf; // opzionale: campo interno solo per noi, nei casi degli altri, calcoliamo noi in pdf, nel caso interno lo passiamo direttamente
    private String uuidMongoFirmato; // opzionale:  campo interno solo per noi, nei casi degli altri, carichiamo su mongo i file, nel caso interno lo passiamo direttamente
    private String uuidMongoOriginale; // opzionale:  campo interno solo per noi, nei casi degli altri, carichiamo su mongo i file, nel caso interno lo passiamo direttamente
    private String nomePartFileOriginale; // obbligatorio uno tra fileOriginale e fileFirmato se non è stato passato nessuno tra uuid_mongo_originale, uuid_mongo_firmato: composto da (filename, valore in base64)
    private String nomePartFileFirmato; // obbligatorio uno tra fileOriginale e fileFirmato se non è stato passato nessuno tra uuid_mongo_originale, uuid_mongo_firmato: composto da (filename, valore in base64)

    private String mimeTypeFileOriginale; // opzionale, campo interno, da passare solo per noi. Comunque, se non passato, viene calcolato in automatico: mime type del file originale
    private String mimeTypeFileFirmato; // opzionale, campo interno, da passare solo per noi. Comunque, se non passato, viene calcolato in automatico: mime type del file firmato

    @JsonIgnore
    private String nomeFileOriginale; // campo interno, non passare

    @JsonIgnore
    private String nomeFileFirmato; // campo interno, non passare
    
    public SottoDocumento() {
    }

    public SottoDocumento(String idOggettoOrigine, String tipoOggettoOrigine, DocumentOperationType tipoOperazione, String codiceSottoDocumemento, String tipo, TipoFirma tipoFirma, List<String> firmatari, boolean principale, String nome, String uuidMongoPdf, String uuidMongoFirmato, String uuidMongoOriginale, String nomePartFileOriginale, String nomePartFileFirmato, String mimeTypeFileOriginale, String mimeTypeFileFirmato) {
        super.idOggettoOrigine = idOggettoOrigine;
        super.tipoOggettoOrigine = tipoOggettoOrigine;
        this.tipoOperazione = tipoOperazione;
        this.codiceSottoDocumento = codiceSottoDocumemento;
        this.tipo = tipo;
        this.tipoFirma = tipoFirma;
        this.firmatari = firmatari;
        this.principale = principale;
        this.nome = nome;
        this.uuidMongoPdf = uuidMongoPdf;
        this.uuidMongoFirmato = uuidMongoFirmato;
        this.uuidMongoOriginale = uuidMongoOriginale;
        this.nomePartFileOriginale = nomePartFileOriginale;
        this.nomePartFileFirmato = nomePartFileFirmato;
        this.mimeTypeFileOriginale = mimeTypeFileOriginale;
        this.mimeTypeFileFirmato = mimeTypeFileFirmato;
    }
    
    
    @JsonIgnore
    public void check(DocumentOperationType tipoOperazioneGdDocContenitore) throws IodaDocumentException {

        // campi obblicatori
        if (getIdOggettoOrigine() == null || getIdOggettoOrigine().equals("")) {
            throw new IodaDocumentException("idOggettoOrigine di un sottoDocumento mancante");
        }
        else if (getTipoOggettoOrigine() == null || getTipoOggettoOrigine().equals("")) {
            throw new IodaDocumentException("tipoOggettoOrigine del sottoDocumento: " + getIdOggettoOrigine() + " mancante");
        }
        else if (getTipoOperazione() == null) {
            throw new IodaDocumentException("tipoOperazione del sottoDocumento: " + getIdOggettoOrigine() + " mancante");
        }
        else if (getCodiceSottoDocumento() == null || getCodiceSottoDocumento().equals("")) {
            throw new IodaDocumentException("codiceSottoDocumento del sottoDocumento: " + getIdOggettoOrigine() + " mancante");
        }
        
        // se l'operazione del GdDoc contenitore è insert allora il campo tipoOperazione di tutti i suoi sottodocumenti deve essere insert
        else if (tipoOperazioneGdDocContenitore == DocumentOperationType.INSERT && getTipoOperazione() != DocumentOperationType.INSERT) {
            throw new IodaDocumentException("tipoOperazione del sottoDocumento: " + getIdOggettoOrigine() + " errata. Se il Documento è in fase di inserimento tutti i SottoDocumenti devono avere tipoOperazione: insert");
        }
        // se l'operazione del GdDoc contenitore è delete allora il campo tipoOperazione di tutti i suoi sottodocumenti deve essere delete
        else if (tipoOperazioneGdDocContenitore == DocumentOperationType.DELETE && getTipoOperazione() != DocumentOperationType.DELETE) {
            throw new IodaDocumentException("tipoOperazione del sottoDocumento: " + getIdOggettoOrigine() + " errata. Se il Documento è in fase di cancellazione tutti i SottoDocumenti devono avere tipoOperazione: delete");
        }
        
        // campi obbligatori solo nel caso di inserimento del SottoDocumento
        else if (tipoOperazione == DocumentOperationType.INSERT && (getTipo() == null || getTipo().equals(""))) {
            throw new IodaDocumentException("tipo del sottoDocumento: " + getIdOggettoOrigine() + " mancante");
        }
        
        // se è un file firmato deve esserci il tipoFirma
        else if (tipoOperazione == DocumentOperationType.INSERT &&
                ((getUuidMongoFirmato() != null && !getUuidMongoFirmato().equals("")) || getNomePartFileFirmato() != null) &&
                (getTipoFirma() == null)) {
            throw new IodaDocumentException("tipoFirma del sottoDocumento: " + getIdOggettoOrigine() + " mancante");
        }
        else if (tipoOperazione == DocumentOperationType.INSERT && (isPrincipale() == null)) {
            throw new IodaDocumentException("principale del sottoDocumento: " + getIdOggettoOrigine() + " mancante");
        }
        else if (tipoOperazione == DocumentOperationType.INSERT && (getNome() == null || getNome().equals(""))) {
            throw new IodaDocumentException("nome del sottoDocumento: " + getIdOggettoOrigine() + " mancante");
        }
        
        // deve esserci almeno un campo tra: uuid_mongo_firmato, uuid_mongo_originale, fileOriginale, fileFirmato;
        else if (tipoOperazione == DocumentOperationType.INSERT && (
                    (getNomePartFileFirmato()== null) &&
                    (getNomePartFileOriginale()== null) &&
                    (getUuidMongoFirmato() == null || getUuidMongoFirmato().equals("")) &&
                    (getUuidMongoOriginale() == null || getUuidMongoOriginale().equals("")))
                ) {
            throw new IodaDocumentException("nome della part del originale/firmato del sottoDocumento: " + getIdOggettoOrigine() + " mancante");
        }
        
        // se c'è un file firmato ci devono essere i firmatari
        else if (tipoOperazione == DocumentOperationType.INSERT &&
                    ((getNomePartFileFirmato() != null) || 
                    (getUuidMongoFirmato() != null && !getUuidMongoFirmato().equals(""))) && 
                    (firmatari == null || firmatari.isEmpty())) {
            throw new IodaDocumentException("firmatari del sottoDocumento: " + getIdOggettoOrigine() + " mancanti");
        }
        
//        if (nomePartFileOriginale != null)
//            nomePartFileOriginale.check();
//        
//        if (nomePartFileFirmato != null)
//            nomePartFileFirmato.check();
    }

    public DocumentOperationType getTipoOperazione() {
        return tipoOperazione;
    }

    public String getCodiceSottoDocumento() {
        return codiceSottoDocumento;
    }

    public void setCodiceSottoDocumento(String codiceSottoDocumento) {
        this.codiceSottoDocumento = codiceSottoDocumento;
    }

    public void setTipoOperazione(DocumentOperationType tipoOperazione) {
        this.tipoOperazione = tipoOperazione;
    }
    
    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public TipoFirma getTipoFirma() {
        return tipoFirma;
    }

    public void setTipoFirma(TipoFirma tipoFirma) {
        this.tipoFirma = tipoFirma;
    }

    public List<String> getFirmatari() {
        return firmatari;
    }

    public void setFirmatari(List<String> firmatari) {
        this.firmatari = firmatari;
    }

    public Boolean isPrincipale() {
        return principale;
    }

    public void setPrincipale(Boolean principale) {
        this.principale = principale;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getUuidMongoPdf() {
        return uuidMongoPdf;
    }

    public void setUuidMongoPdf(String uuidMongoPdf) {
        this.uuidMongoPdf = uuidMongoPdf;
    }

    public String getUuidMongoFirmato() {
        return uuidMongoFirmato;
    }

    public void setUuidMongoFirmato(String uuidMongoFirmato) {
        this.uuidMongoFirmato = uuidMongoFirmato;
    }

    public String getUuidMongoOriginale() {
        return uuidMongoOriginale;
    }

    public void setUuidMongoOriginale(String uuidMongoOriginale) {
        this.uuidMongoOriginale = uuidMongoOriginale;
    }

    public String getNomePartFileOriginale() {
        return nomePartFileOriginale;
    }

    public void setNomePartFileOriginale(String nomePartFileOriginale) {
        this.nomePartFileOriginale = nomePartFileOriginale;
    }

    public String getNomePartFileFirmato() {
        return nomePartFileFirmato;
    }

    public void setNomePartFileFirmato(String nomePartFileFirmato) {
        this.nomePartFileFirmato = nomePartFileFirmato;
    }

    public String getMimeTypeFileOriginale() {
        return mimeTypeFileOriginale;
    }

    public void setMimeTypeFileOriginale(String mimeTypeFileOriginale) {
        this.mimeTypeFileOriginale = mimeTypeFileOriginale;
    }

    public String getMimeTypeFileFirmato() {
        return mimeTypeFileFirmato;
    }

    public void setMimeTypeFileFirmato(String mimeTypeFileFirmato) {
        this.mimeTypeFileFirmato = mimeTypeFileFirmato;
    }

    @JsonIgnore
    public String getNomeFileOriginale() {
        return nomeFileOriginale;
    }

    @JsonIgnore
    public void setNomeFileOriginale(String nomeFileOriginale) {
        this.nomeFileOriginale = nomeFileOriginale;
    }

    @JsonIgnore
    public String getNomeFileFirmato() {
        return nomeFileFirmato;
    }

    @JsonIgnore
    public void setNomeFileFirmato(String nomeFileFirmato) {
        this.nomeFileFirmato = nomeFileFirmato;
    }
}
