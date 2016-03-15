package it.bologna.ausl.bds_tools.ioda.utils;

import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.ioda.iodaobjectlibrary.ClassificazioneFascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolazione;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolazioni;
import it.bologna.ausl.ioda.iodaobjectlibrary.SimpleDocument;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class IodaFascicolazioniUtilities {
    
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(IodaFascicolazioniUtilities.class);
    
    private String gdDocTable;
    private String fascicoliTable;
    private String titoliTable;
    private String fascicoliGdDocTable;
    private String utentiTable;
    private String prefixIds; // prefisso da anteporre agli id dei documenti che si inseriscono o che si ricercano (GdDoc, SottoDocumenti)
    private SimpleDocument sd;
    HttpServletRequest request;
    
    private IodaFascicolazioniUtilities(SimpleDocument sd, String prefixIds) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this.gdDocTable = ApplicationParams.getGdDocsTableName();
        this.fascicoliTable = ApplicationParams.getFascicoliTableName();
        this.titoliTable = ApplicationParams.getTitoliTableName();
        this.fascicoliGdDocTable = ApplicationParams.getFascicoliGdDocsTableName();
        this.utentiTable = ApplicationParams.getUtentiTableName();
        this.prefixIds = prefixIds;
        this.sd = sd;
        sd.setPrefissoApplicazioneOrigine(this.prefixIds);
        
    }
    
    public IodaFascicolazioniUtilities(HttpServletRequest request, SimpleDocument sd, String prefixIds) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this(sd, prefixIds);
        this.request = request;
    }
    
    
    public Fascicolazioni getFascicolazioni(Connection dbConn, PreparedStatement ps) throws SQLException{
        
        Fascicolazioni fascicolazioni = new Fascicolazioni(sd.getIdOggettoOrigine(), sd.getTipoOggettoOrigine());
        
        // estrazione dei fascicoli del documento
        ArrayList<String> idFascicoli = this.getIdFascicoli(dbConn, ps);
        
        // per ogni fascicolo calcolo la sua classificazione
        for (String idFascicolo : idFascicoli) {
            ClassificazioneFascicolo classificazioneFascicolo = this.getClassificazioneFascicolo(dbConn, ps, idFascicolo);
            Fascicolazione fascicolazione = getFascicolazione(dbConn, ps, idFascicolo, classificazioneFascicolo);
            fascicolazioni.addFascicolazione(fascicolazione);
        }
        return fascicolazioni;
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
    
    private ArrayList<String> getIdFascicoli(Connection dbConn, PreparedStatement ps) throws SQLException {
        
        ArrayList<String> res = new ArrayList<>(); 
        
        String sqlText =
                    "SELECT fg.id_fascicolo " +
                    "FROM " + getGdDocTable() + " g, " + getFascicoliGdDocTable() + " fg  " +
                    "WHERE g.id_oggetto_origine = ? " +
                    "AND g.tipo_oggetto_origine = ? " +
                    "AND g.id_gddoc = fg.id_gddoc " + 
                    "AND fg.data_eliminazione IS NULL ";

        ps = dbConn.prepareStatement(sqlText);
        int index = 1;
        ps.setString(index++, sd.getIdOggettoOrigine());
        ps.setString(index++, sd.getTipoOggettoOrigine());
        
        String query = ps.toString();
        log.debug("eseguo la query: " + query + " ...");
        ResultSet results = ps.executeQuery();
        
        while (results.next()) {
            res.add(results.getString(1));
        }
        return res;
    }
     
    
    
    private ClassificazioneFascicolo getClassificazioneFascicolo(Connection dbConn, PreparedStatement ps, String idFascicolo) throws SQLException{
        
        ClassificazioneFascicolo classificazioneFascicolo = null;
        
        String sqlText = 
                "SELECT t.codice_gerarchico, t.codice_titolo " +
                "FROM " + getFascicoliTable() + " f, " + getTitoliTable() + " t " +
                "WHERE id_fascicolo = ? " +
                "AND f.id_titolo = t.id_titolo ";          
      
        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idFascicolo);
        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();
        
        String result;
        
        if (!res.next())
            throw new SQLException("titolo non trovato");
        else{
            result = res.getString(1) + res.getString(2);
            
            classificazioneFascicolo = new ClassificazioneFascicolo();
            
            // calcolo la gerarchia e setta il corrispettivo nome
            String[] parts = result.split("-");
            int dim = parts.length;
            
            while(dim>0){
                switch(dim){
                case 3:
                    String nomeSottoClasse = this.getTitolo(dbConn, ps, parts[0] + "-" + parts[1] + "-", parts[2]);
                    classificazioneFascicolo.setNomeSottoclasse(nomeSottoClasse);
                    classificazioneFascicolo.setCodiceSottoclasse(parts[0] + "-" + parts[1] + "-" + parts[2]);
                    break;

                case 2:
                    String nomeClasse = this.getTitolo(dbConn, ps, parts[0] + "-", parts[1]);
                    classificazioneFascicolo.setNomeClasse(nomeClasse);
                    classificazioneFascicolo.setCodiceClasse(parts[0] + "-" + parts[1]);
                    break;

                case 1:
                    String nomeCategoria = this.getTitolo(dbConn, ps, null, parts[0]);
                    classificazioneFascicolo.setNomeCategoria(nomeCategoria);
                    classificazioneFascicolo.setCodiceCategoria(parts[0]);
                    break;
                }
                dim--;
            }  
        }
        return classificazioneFascicolo;
    }
    
    
    private Fascicolazione getFascicolazione(Connection dbConn, PreparedStatement ps, String idFascicolo, ClassificazioneFascicolo classificazione) throws SQLException{
        
        Fascicolazione fascicolazione = null;
        
        String sqlTextOld = 
                "SELECT f.numerazione_gerarchica, f.nome_fascicolo, fg.data_assegnazione, fg.id_utente_fascicolatore, fg.data_eliminazione, fg.id_utente_eliminazione " +
                "FROM " + getFascicoliTable() + " f, " + getFascicoliGdDocTable() + " fg, "+ getGdDocTable() + " g " +
                "WHERE g.id_oggetto_origine = ? " +
                "AND g.tipo_oggetto_origine = ? " +
                "AND g.id_gddoc = fg.id_gddoc " +
                "AND f.id_fascicolo = fg.id_fascicolo " +
                "AND fg.id_fascicolo = ? ";
	
        String sqlText = 
                "SELECT f.numerazione_gerarchica, f.nome_fascicolo, fg.data_assegnazione, fg.id_utente_fascicolatore, fg.data_eliminazione, " +
                "fg.id_utente_eliminazione, " +
                "CASE f.id_livello_fascicolo WHEN '2' THEN (select nome_fascicolo from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo) " +
                "WHEN '3' THEN (select nome_fascicolo from gd.fascicoligd where id_fascicolo = (select id_fascicolo_padre from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo)) " +
                "ELSE nome_fascicolo " +
                "END as nome_fascicolo_interfaccia " +
                "FROM " + getFascicoliTable() + " f, " + getFascicoliGdDocTable() + " fg, "+ getGdDocTable() + " g " +
                "WHERE g.id_oggetto_origine = ? " +
                "AND g.tipo_oggetto_origine = ? " +
                "AND g.id_gddoc = fg.id_gddoc " +
                "AND f.id_fascicolo = fg.id_fascicolo " +
                "AND fg.id_fascicolo = ? " + 
                "AND fg.data_eliminazione IS NULL ";
        
        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, sd.getIdOggettoOrigine());
        ps.setString(2, sd.getTipoOggettoOrigine());
        ps.setString(3, idFascicolo);
        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();
        
        if (!res.next())
            throw new SQLException("fascicolazione non trovata");
        else{
            int index = 1;
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
            String numerazioneGerarchica = res.getString(index++);
            String nomeFascicolo = res.getString(index++);
            DateTime dataAssegnazione;
            String dataStr = res.getString(index++);
            try{
                
                dataAssegnazione = DateTime.parse(dataStr, formatter);
            } catch(Exception e){
                formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                dataAssegnazione = DateTime.parse(dataStr, formatter);
            }
            
            String idUtenteFascicolatore = res.getString(index++);
            
            // controllo che esista la data di eliminazione
            String controlloDataEliminazione = res.getString(index++);
            DateTime dataEliminazione = null;
            if(controlloDataEliminazione != null && !controlloDataEliminazione.equals(""))
                dataEliminazione = DateTime.parse(res.getString(index++), formatter);
            
            
            // controllo che esista idUtenteEliminatore
            String idUtenteEliminatore = res.getString(index++);
            String nomeFascicoloInterfaccia = res.getString(index++);
            String descrizioneEliminatore = null;
            boolean eliminato = false;
            if(idUtenteEliminatore != null && !idUtenteEliminatore.equals("")){
                eliminato = true;
                descrizioneEliminatore = this.getNomeCognome(dbConn, ps, idUtenteEliminatore);
            }
            String descrizioneFascicolatore = this.getNomeCognome(dbConn, ps, idUtenteFascicolatore);         
            
           
            fascicolazione = new Fascicolazione(numerazioneGerarchica, nomeFascicolo, idUtenteFascicolatore, descrizioneFascicolatore, dataAssegnazione, eliminato, dataEliminazione, idUtenteEliminatore, descrizioneEliminatore, classificazione, nomeFascicoloInterfaccia);
           
        }
        return fascicolazione;
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
