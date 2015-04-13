/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author utente
 */
public class SottoDocumento extends Document{

    enum TipoFirma {
        GRAFOMETRICA("grafometrica"),
        DIGITALE("digitale"),
        AUTOGRAFA("autografa");

        private String key;

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

    private String tipo; // tipo del sotto documento. Il tipo deve essere concordato con noi. Come con i registri
    private TipoFirma tipoFirma; // tipologia di firma: grafometrica, digitale
    private List<String> firmatari;
    private String mimeType; // del singolo 
    private boolean principale;
    private String codice;// codice identificativo del documento passatoci
    private String nome;
    private String uuid_mongo_pdf; // campo interno solo per noi, nei casi degli altri, calcoliamo noi in pdf, nel caso interno lo passiamo direttamente
    private String uuid_mongo_firmato; // campo interno solo per noi, nei casi degli altri, carichiamo su mongo i file, nel caso interno lo passiamo direttamente
    private String uuid_mongo_originale; // campo interno solo per noi, nei casi degli altri, carichiamo su mongo i file, nel caso interno lo passiamo direttamente
    private String fileOriginale; // // (in base64) viene popolato quando arrivano dall'esterno
    private String fileFirmato; // (in base64) viene popolato quando arrivano dall'esterno
    // almeno uno tra 'fileOriginale' e 'fileFirmato' ci deve essere

    public SottoDocumento() {
    }

    public SottoDocumento(String tipo, TipoFirma tipoFirma, List<String> firmatari, String mimeType, boolean principale, String codice, String nome, String uuid_mongo_pdf, String uuid_mongo_firmato, String uuid_mongo_originale, String fileOriginale, String fileFirmato) {
        this.tipo = tipo;
        this.tipoFirma = tipoFirma;
        this.firmatari = firmatari;
        this.mimeType = mimeType;
        this.principale = principale;
        this.codice = codice;
        this.nome = nome;
        this.uuid_mongo_pdf = uuid_mongo_pdf;
        this.uuid_mongo_firmato = uuid_mongo_firmato;
        this.uuid_mongo_originale = uuid_mongo_originale;
        this.fileOriginale = fileOriginale;
        this.fileFirmato = fileFirmato;
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

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public boolean isPrincipale() {
        return principale;
    }

    public void setPrincipale(boolean principale) {
        this.principale = principale;
    }

    public String getCodice() {
        return codice;
    }

    public void setCodice(String codice) {
        this.codice = codice;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getUuid_mongo_pdf() {
        return uuid_mongo_pdf;
    }

    public void setUuid_mongo_pdf(String uuid_mongo_pdf) {
        this.uuid_mongo_pdf = uuid_mongo_pdf;
    }

    public String getUuid_mongo_firmato() {
        return uuid_mongo_firmato;
    }

    public void setUuid_mongo_firmato(String uuid_mongo_firmato) {
        this.uuid_mongo_firmato = uuid_mongo_firmato;
    }

    public String getUuid_mongo_originale() {
        return uuid_mongo_originale;
    }

    public void setUuid_mongo_originale(String uuid_mongo_originale) {
        this.uuid_mongo_originale = uuid_mongo_originale;
    }

    public String getFileOriginale() {
        return fileOriginale;
    }

    public void setFileOriginale(String fileOriginale) {
        this.fileOriginale = fileOriginale;
    }

    public String getFileFirmato() {
        return fileFirmato;
    }

    public void setFileFirmato(String fileFirmato) {
        this.fileFirmato = fileFirmato;
    }

    
}
