package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.exceptions.VersatoreParerException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.DatiParerGdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolazione;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.PubblicazioneIoda;
import it.bologna.ausl.ioda.iodaobjectlibrary.Requestable;
import it.bologna.ausl.ioda.iodaobjectlibrary.SimpleDocument;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;


/**
 *
 * @author andrea
 */
@DisallowConcurrentExecution
public class VersatoreParer implements Job {

    private static final Logger log = LogManager.getLogger(VersatoreParer.class);
    
    private static final String GENERATE_INDE_NUMBER_PARAM_NAME = "generateidnumber";
    private static final String INDE_DOCUMENT_ID_PARAM_NAME = "document_id";
 
    private boolean canSendPicoUscita, canSendPicoEntrata, canSendDete, canSendDeli;
    private boolean canSendRegistroGiornaliero, canSendRegistroAnnuale;
    private int limit;
    private String idApplicazione, tokenApplicazione;
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.debug("Versatore ParER Started");

        try {
            
            // ottengo i gddoc che possono essere versati
            ArrayList<String> gdDocList = getIdGdDocDaVersate();
            
            // se ho qualche gddoc da versare
            if (gdDocList != null && gdDocList.size() > 0){
            
                // inizio sessione di versamento
                String idSessioneVersamento = setStartSessioneVersamento();
                if (idSessioneVersamento != null && !idSessioneVersamento.equals("")){

                    // ottengo l'oggetto GdDoc completo
                    for (String idGdDoc : gdDocList) {
                        GdDoc gddoc = getGdDocById(idGdDoc);
                         
                        DateTime d = getDataPrimaFascicolazione(gddoc.getFascicolazioni());
                        log.debug("DATA PRIMA: " + d.toString());
                        
//                        List<PubblicazioneIoda> pubblicazioni = gddoc.getPubblicazioni();
//                        for(PubblicazioneIoda p : pubblicazioni){
//                            log.debug("Numero pubblicazione: " + p.getNumeroPubblicazione());
//                        }
                        
//                        List<Fascicolazione> array = gddoc.getFascicolazioni();
//                        for (Fascicolazione fascicolazione : array) {
//                            
//                            log.debug("Nome fascicolo: " + fascicolazione.getNomeFascicolo());
//                        }
                    }
                    
                }
                
            }
            
          
            //        Connection dbConn = null;
//        PreparedStatement ps = null;
//        try {
//            dbConn = UtilityFunctions.getDBConnection();
//            log.debug("connessione db ottenuta");
//
//            dbConn.setAutoCommit(false);
//            log.debug("autoCommit settato a: " + dbConn.getAutoCommit());
//
//
//            
//            // se tutto è ok faccio il commit
//            dbConn.commit();
//        } //try
//        catch (Throwable t) {
//            log.fatal("Errore nel versatore parER", t);
//            try {
//                if (dbConn != null) {
//                    log.info("rollback...");
//                    dbConn.rollback();
//                }
//            }
//            catch (Exception ex) {
//                log.error("errore nel rollback", ex);
//            }
//            throw new JobExecutionException(t);
//        }
//        finally {
//            if (ps != null) {
//                try {
//                    ps.close();
//                } catch (Exception ex) {
//                    log.error(ex);
//                }
//            }
//            if (dbConn != null) {
//                try {
//                    dbConn.close();
//                } catch (Exception ex) {
//                    log.error(ex);
//                }
//            }
            log.debug("valore canSendPico: " + getCanSendPicoUscita());
            Thread.sleep(5000);
        }catch (Throwable t) {
            log.fatal("Versatore Parer: Errore ...", t);
        }
        finally {
            log.info("Job Versatore Parer finished");
        }

        log.debug("Versatore ParER Ended");
    }
    
    
    private ArrayList<String> getIdGdDocDaVersate() throws SQLException, NamingException, VersatoreParerException{
        
        String query = null;
        ArrayList<String> res = new ArrayList<>();
        
        if (getLimit() > 0){
            query = 
                "SELECT id_gddoc " +
                "FROM " + ApplicationParams.getDatiParerGdDocTableName() + " " +
                "WHERE (stato_versamento_effettivo = 'non_versare' or stato_versamento_effettivo = 'errore_versamento') " + 
                "and xml_specifico_parer IS NOT NULL and xml_specifico_parer !=  '' " + 
                "and (stato_versamento_proposto = 'da_versare' or stato_versamento_proposto = 'da_aggiornare') " + 
                "limit " + getLimit();
        }
        else{
            query = 
                "SELECT id_gddoc " +
                "FROM " + ApplicationParams.getDatiParerGdDocTableName() + " " +
                "WHERE (stato_versamento_effettivo = 'non_versare' or stato_versamento_effettivo = 'errore_versamento') " + 
                "and xml_specifico_parer IS NOT NULL and xml_specifico_parer !=  '' " + 
                "and (stato_versamento_proposto = 'da_versare' or stato_versamento_proposto = 'da_aggiornare') ";
        }      
        
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(query)
        ) {
            if (dbConnection != null) {
                log.debug("dbConnection is not null");
            }else{
                log.debug("dbConnction == null");
            }
            
            log.debug("ottengo id dei gddoc da versare");
            
            log.debug("PrepareStatment: " + ps);
            ResultSet resultQuery = ps.executeQuery();
            
            while (resultQuery.next()) {
                res.add(resultQuery.getString("id_gddoc"));
            }
        }
        catch(Exception ex) {
            throw new VersatoreParerException("Errore nel reperimento dei gddoc da inviare", ex);
        }
        return res;
    }
    
    private String setStartSessioneVersamento(){
        
        String res = null;
                
        String query = 
            "INSERT INTO " + ApplicationParams.getSessioniVersamentoParerTableName() + "( " +
            "id_sessione_versamento_parer, data_inizio) " + 
            "VALUES (?, date_trunc('sec', now()::timestamp)) ";
            
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(query)
        ) {         
            log.debug("setto data e ora inizio sessione di versamento");
            
            // ottengo in ID di INDE
            String idInde = getIndeId();
            log.debug("valore id INDE: " + idInde);
            
            int index = 1;
            
            // ID di INDE
            ps.setString(index++, idInde);
                                    
            log.debug("PrepareStatment: " + ps);                      
            int rowsUpdated = ps.executeUpdate();
            log.debug("eseguita");
            if (rowsUpdated == 0)
                throw new SQLException("data inizio non inserita");
            else
                res = idInde;
        } catch (SQLException | NamingException | IOException | SendHttpMessageException ex) {
            log.fatal("errore inserimento data_inizio nella tabella sessioni_versamento_parer");
        }
        return res;
    }
    
    // restituisce un id di INDE
    private String getIndeId() throws IOException, MalformedURLException, SendHttpMessageException {
            
        // contruisco la mappa dei parametri per la servlet (va passato solo un parametro che è il numero di id da generare)
        Map<String, byte[]> params = new HashMap<>();

        // ottengo un docid
        int idNumber = 1; 
        params.put(GENERATE_INDE_NUMBER_PARAM_NAME, String.valueOf(idNumber).getBytes());

        String res = UtilityFunctions.sendHttpMessage(ApplicationParams.getGetIndeUrlServiceUri(), null, null, params, "POST", null);

        // la servlet torna un JsonObject che è una coppia (id, guid)
        JSONArray indeIdArray = (JSONArray) JSONValue.parse(res);
        JSONObject currentId = (JSONObject) indeIdArray.get(0);
        return (String) currentId.get(INDE_DOCUMENT_ID_PARAM_NAME);
    }
    
    private String getMovimentazione(String xmlSpecifico){
        
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(xmlSpecifico);
        String movimentazione = (String) jsonXmlSpecifico.get("movimentazione");
        return movimentazione;
    }
    
    private boolean isGdDocVersabile(DatiParerGdDoc datiParer){
        
        DateTime dataRegistrazione = null;
        
//        String query = 
//                "SELECT data_registrazione, " +
//                "FROM " + ApplicationParams.getGdDocsTableName() + " " +
//                "WHERE id_gddoc = ? ";
//        
//        try (
//            Connection dbConnection = UtilityFunctions.getDBConnection();
//            PreparedStatement ps = dbConnection.prepareStatement(query)
//        ) {
//            log.debug("verifica sei il gddoc è versabile");
//            
//            log.debug("PrepareStatment: " + ps);
//            ResultSet resultQuery = ps.executeQuery();
//            
//            while (resultQuery.next()) {
//                Timestamp dataRegistrazioneTS = resultQuery.getTimestamp("data_registrazione");
//                if (dataRegistrazioneTS != null)
//                    dataRegistrazione = new DateTime(dataRegistrazioneTS.getTime());
//            }
//            
//            
//        }
//        catch(Exception ex) {
//            throw new VersatoreParerException("Errore nel reperimento dei gddoc da inviare", ex);
//        }
        return false;
    }
    
    /* crea l'oggetto GdDoc con le collection:
    * CLASSIFICAZIONI esclusi i fascicoli speciali
    * PUBBLICAZIONI solo quelle effettivamente pubblicate all'albo
    */
    private GdDoc getGdDocById(String idGdDoc) throws SQLException, NamingException, NotAuthorizedException, IodaDocumentException{
        
        String prefix;
        
        GdDoc gdDoc = null;
        
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
        ) {
            prefix = UtilityFunctions.checkAuthentication(dbConnection, getIdApplicazione(), getTokenApplicazione());
            
            HashMap<String, Object> additionalData = new HashMap <String, Object>(); 
            additionalData.put("FASCICOLAZIONI", "LOAD");
            additionalData.put("PUBBLICAZIONI", "LOAD");
            
            
            gdDoc = IodaDocumentUtilities.getGdDocById(dbConnection, idGdDoc, additionalData, prefix);
        }
        return gdDoc;    
    }
    
    
    private boolean isVersabile(){
        
        boolean res = false;
        
        
        
        return res;
    }
        
    
    private DateTime getDataPrimaFascicolazione(List<Fascicolazione> fascicolazioni){
        
        DateTime res = null;
        
        if (fascicolazioni != null){
            if (fascicolazioni.size() > 0){
                
                // ordino in ordine crescente
                Collections.sort(fascicolazioni);
                
                res = fascicolazioni.get(0).getDataFascicolazione();
            }
            else{
                log.debug("fascicolazioni = 0");
            }
        }
        else{
            log.debug("fascicolazioni = null");
        }
        
        return res;
    }
    
    
    public boolean getCanSendPicoUscita() {
        return canSendPicoUscita;
    }

    public void setCanSendPicoUscita(boolean canSendPicoUscita) {
        this.canSendPicoUscita = canSendPicoUscita;
    }
    
    public boolean getCanSendPicoEntrata() {
        return canSendPicoEntrata;
    }

    public void setCanSendPicoEntrata(boolean canSendPicoEntrata) {
        this.canSendPicoEntrata = canSendPicoEntrata;
    }
    
    public boolean getCanSendDete() {
        return canSendDete;
    }

    public void setCanSendDete(boolean canSendDete) {
        this.canSendDete = canSendDete;
    }
    
    public boolean getCanSendDeli() {
        return canSendDeli;
    }

    public void setCanSendDeli(boolean canSendDeli) {
        this.canSendDeli = canSendDeli;
    }
    
    public boolean getCanSendRegistroGiornaliero() {
        return canSendRegistroGiornaliero;
    }

    public void setCanSendRegistroGiornaliero(boolean canSendRegistroGiornaliero) {
        this.canSendRegistroGiornaliero = canSendRegistroGiornaliero;
    }
    
    public boolean getCanSendRegistroAnnuale() {
        return canSendRegistroAnnuale;
    }

    public void setCanSendRegistroAnnuale(boolean canSendRegistroAnnuale) {
        this.canSendRegistroAnnuale = canSendRegistroAnnuale;
    }
    
    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
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
    
}
