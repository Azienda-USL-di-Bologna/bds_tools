package it.bologna.ausl.bds_tools.ioda.utils;

import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicoli;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.Researcher;
import it.bologna.ausl.ioda.iodaoblectlibrary.exceptions.IodaDocumentException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class IodaFascicoliUtilities {
    
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(IodaFascicolazioniUtilities.class);
    
    private String gdDocTable;
    private String fascicoliTable;
    private String titoliTable;
    private String fascicoliGdDocTable;
    private String utentiTable;
    private String prefixIds; // prefisso da anteporre agli id dei documenti che si inseriscono o che si ricercano (GdDoc, SottoDocumenti)
    private Researcher researcher;
    HttpServletRequest request;
    
    private IodaFascicoliUtilities(ServletContext context, Researcher r) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this.gdDocTable = context.getInitParameter("GdDocsTableName");
        this.fascicoliTable = context.getInitParameter("FascicoliTableName");
        this.titoliTable = context.getInitParameter("TitoliTableName");
        this.fascicoliGdDocTable = context.getInitParameter("FascicoliGdDocsTableName");
        this.utentiTable = context.getInitParameter("UtentiTableName");
        this.researcher = r;
    }
    
    public IodaFascicoliUtilities(ServletContext context, HttpServletRequest request, Researcher r) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this.request = request;
        this.researcher = r;
    }
    
    
    public Fascicoli getFascicoli(Connection dbConn, PreparedStatement ps) throws SQLException{
        Fascicoli fascicoli = getFascicoli(dbConn, ps, researcher.getSearchString(), researcher.getIdUtente());
        
        return fascicoli;
    }
    
    
    
    private Fascicoli getFascicoli(Connection dbConn, PreparedStatement ps, String strToFind, String idUtente) throws SQLException{
        
        Fascicoli res = new Fascicoli();
        
        String sqlText = "select distinct(f.id_fascicolo), f.id_livello_fascicolo, f.numero_fascicolo, " + 
                         "f.nome_fascicolo,f.anno_fascicolo, f.id_utente_creazione, f.data_creazione, " +
                         "f.numerazione_gerarchica, f.id_utente_responsabile, uResp.nome, uResp.cognome, " + 
                         "f.stato_fascicolo, f.id_utente_responsabile_proposto, f.id_fascicolo_padre, " + 
                         "f.id_titolo, f.speciale, t.codice_gerarchico || '' || t.codice_titolo || ' ' || t.titolo as titolo, " +
                         "case when fv.id_fascicolo is null then 0 else -1 end as accesso " +
                         "from " +
                         "gd.fascicoligd f " +
                         "left join procton.titoli t on t.id_titolo = f.id_titolo " +
                         "left join gd.log_azioni_fascicolo laf on laf.id_fascicolo= f.id_fascicolo " +
                         "join gd.fascicoli_modificabili fv on fv.id_fascicolo=f.id_fascicolo and fv.id_utente= ? " +
                         "join procton.utenti uResp on uResp.id_utente=f.id_utente_responsabile " +
                         "join procton.utenti uCrea on f.id_utente_creazione=uCrea.id_utente " +
                         "where 1=1  AND (f.nome_fascicolo ilike ('%" + strToFind + "%') " +
                         "or cast(f.numero_fascicolo as text) like '%" + strToFind + "%') " + 
                         "and f.numero_fascicolo != '0' and f.speciale != -1 and f.stato_fascicolo != 'p' " + 
                         "and f.stato_fascicolo != 'c' order by f.nome_fascicolo";
        
        ps = dbConn.prepareStatement(sqlText);
        
        ps.setString(1, idUtente);
        log.debug("sql: " + ps.toString());
        
        ResultSet results = ps.executeQuery();
        
        while (results.next()) {
            int index = 2;
            
            String idLivelloFascicolo = results.getString(index++);
            int numeroFascicolo = results.getInt(index++);
            String nomeFascicolo = results.getString(index++);
            int annoFascicolo = results.getInt(index++);
            String idUtenteCreazione = results.getString(index++);
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dataCreazione = DateTime.parse(results.getString(index++), formatter);
            String numerazioneGerarchica = results.getString(index++);
            String idUtenteResponsabile = results.getString(index++);
            String nomeUtenteResponsabile = results.getString(index++);
            String cognomeUtenteResponsabile = results.getString(index++);
            String statoFascicolo = results.getString(index++);
            String idUtenteResponsabileProposto = results.getString(index++);
            String idFascicoloPadre = results.getString(index++);
            String idTitolo = results.getString(index++);
            int speciale = results.getInt(index++);
            String titolo = results.getString(index++);
            int accesso = results.getInt(index++);
            
            
            String descrizioneUtenteResponsabile = nomeUtenteResponsabile + " " + cognomeUtenteResponsabile;
            Fascicolo f = new Fascicolo();
            f.setCodiceFascicolo(numerazioneGerarchica);
            f.setNumeroFascicolo(numeroFascicolo);
            f.setNomeFascicolo(nomeFascicolo);
            f.setIdUtenteResponsabile(idUtenteResponsabile);
            f.setDescrizioneUtenteResponsabile(descrizioneUtenteResponsabile);
            f.setDataCreazione(dataCreazione);
            f.setAnnoFascicolo(annoFascicolo);
            f.setIdLivelloFascicolo(idLivelloFascicolo);
            f.setIdUtenteCreazione(idUtenteCreazione);
            f.setStatoFascicolo(statoFascicolo);
            f.setSpeciale(speciale);
            f.setTitolo(titolo);
            f.setAccesso(accesso);
            f.setIdUtenteResponsabileProposto(idUtenteResponsabileProposto);
            
            res.addFascicolo(f);
        }
        
        return res;
    }
    
    
    
    
    
    
    
    
        
    private String getTitolo(Connection dbConn, PreparedStatement ps, String codiceGerarchico, String codiceTitolo) throws SQLException{
        
        String sqlText = "SELECT titolo " +
                  "FROM " + getTitoliTable() + " " + 
                  "WHERE codice_gerarchico = ? " +
                  "AND codice_titolo = ? ";
        
        ps = dbConn.prepareStatement(sqlText);
        
        if(codiceGerarchico != null && !"".equals(codiceGerarchico)){
            ps.setString(1, codiceGerarchico);
        }
        else{
            ps.setString(1, "");
        }
        
        ps.setString(2, codiceTitolo);
        
        
        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();
        
        if (!res.next())
            throw new SQLException("nessun titolo trovato");
        return res.getString(1);
    }
    
    
    
    public String getNomeCognome(Connection dbConn, PreparedStatement ps, String idUtente) throws SQLException{
        
        String result = null;
        
        String sqlText = 
                "SELECT nome, cognome " +
                "FROM " + getUtentiTable() + " " +
                "WHERE id_utente = ? ";          
      
        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idUtente);
        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();
                
        if (!res.next())
            throw new SQLException("utente non trovato");
        else
            result = res.getString(1) + " " + res.getString(2);
        
        return result;
    }

    public String getGdDocTable() {
        return gdDocTable;
    }
    
    public String getTitoliTable() {
        return titoliTable;
    }
    
    public String getFascicoliTable() {
        return fascicoliTable;
    }
    
    public String getFascicoliGdDocTable() {
        return fascicoliGdDocTable;
    }
    
    public String getUtentiTable() {
        return utentiTable;
    }
}
