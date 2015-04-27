package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.annotation.JsonIgnore;
import it.bologna.ausl.bds_tools.ioda.utils.exceptions.IodaDocumentException;

/**
 *
 * @author gdm
 */
public class Fascicolo {
    private String codiceFascicolo; // obbligatorio: codice che identifica il univocamente il fascicolo
    private Document.DocumentOperationType tipoOperazione; // obbligatorio; operazione da effettuare (insert/delete)

    public Fascicolo() {
    }

    public Fascicolo(String codiceFascicolo, Document.DocumentOperationType tipoOperazione) {
        this.codiceFascicolo = codiceFascicolo;
        this.tipoOperazione = tipoOperazione;
    }

    public String getCodiceFascicolo() {
        return codiceFascicolo;
    }

    public void setCodiceFascicolo(String codiceFascicolo) {
        this.codiceFascicolo = codiceFascicolo;
    }

    public Document.DocumentOperationType getTipoOperazione() {
        return tipoOperazione;
    }

    public void setTipoOperazione(Document.DocumentOperationType tipoOperazione) {
        this.tipoOperazione = tipoOperazione;
    }
    
    @JsonIgnore
    public void check(Document.DocumentOperationType tipoOperazioneGdDocContenitore) throws IodaDocumentException {
        if (getCodiceFascicolo() == null) {
            throw new IodaDocumentException("codice del fascicolo: " + toString() + " mancante");
        }
        else if (getTipoOperazione() == null) {
            throw new IodaDocumentException("tipoOperazione del fascicolo: " + toString()+ " mancante");
        }
        else if (getTipoOperazione() != Document.DocumentOperationType.INSERT && getTipoOperazione() != Document.DocumentOperationType.DELETE) {
             throw new IodaDocumentException("tipoOperazione " + getTipoOperazione() + " del fascicolo: " + toString() + " non ammessa");
        }
        else if (tipoOperazioneGdDocContenitore == Document.DocumentOperationType.INSERT && getTipoOperazione() != Document.DocumentOperationType.INSERT) {
             throw new IodaDocumentException("tipoOperazione del fascicolo: " + toString() + " errata. Se il Documento è in fase di inserimento tutti i fascicoli devono avere tipoOperazione: insert");
        }
        else if (tipoOperazioneGdDocContenitore == Document.DocumentOperationType.DELETE && getTipoOperazione() != Document.DocumentOperationType.DELETE) {
             throw new IodaDocumentException("tipoOperazione del fascicolo: " + toString() + " errata. Se il Documento è in fase di cancellazione tutti i fascicoli devono avere tipoOperazione: delete");
        }
    }

    @Override
    public String toString() {
        return getCodiceFascicolo();
    }
}
