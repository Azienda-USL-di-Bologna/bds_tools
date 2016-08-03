package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.exceptions.VersatoreParerException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.ClassificazioneFascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.DatiParerGdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolazione;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.PubblicazioneIoda;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import it.bologna.ausl.masterchefclient.SendToParerParams;
import it.bologna.ausl.riversamento.builder.ProfiloArchivistico;
import it.bologna.ausl.riversamento.builder.oggetti.DatiSpecifici;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.naming.NamingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
    
    private int versione;
    private String ambiente, ente, strutturaVersante, userID;
    private String tipoComponenteDefault, codifica;
    private boolean useFakeId;
    
    private String idApplicazione, tokenApplicazione;
    
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.debug("Versatore ParER Started");

//        try {
//            // ottengo i gddoc che possono essere versati
//            ArrayList<String> gdDocList = getIdGdDocDaVersate();
//// --------------- parte buona ------------------------------          
//            // se ho qualche gddoc da versare
////            if (gdDocList != null && gdDocList.size() > 0){
////            
////                // inizio sessione di versamento
////                String idSessioneVersamento = setStartSessioneVersamento();
////                if (idSessioneVersamento != null && !idSessioneVersamento.equals("")){
////
////                    boolean isVersabile = false;
//         
//// --------------- fine parte buona ------------------------------                    
//                    
//                    // Ji_j2A?]O6cff-_@[NiY non foglia
//                    // `?71,[j(Fx/SXigr0-;G test generico
//                    // G>WJlH^^1^73T?YzJo5i vecchio PU
//                    GdDoc gddoc = getGdDocById("`?71,[j(Fx/SXigr0-;G");
//                                      
//                    String nomeTitolo = null, codiceTitolo = null;  
//                    for (Fascicolazione fascicolazione : gddoc.getFascicolazioni()) {
//                        
//                        log.debug("Nome Fascicolo: " + fascicolazione.getNomeFascicolo());
//                        ClassificazioneFascicolo classificazione = fascicolazione.getClassificazione();
//                        
//                        if (classificazione.getCodiceSottoclasse() != null && !classificazione.getCodiceSottoclasse().equals("")){
//                            codiceTitolo = classificazione.getCodiceSottoclasse();
//                            nomeTitolo = classificazione.getNomeSottoclasse();
//                        }
//                        else if (classificazione.getCodiceClasse()!= null && !classificazione.getCodiceClasse().equals("")){
//                            codiceTitolo = classificazione.getCodiceClasse();
//                            nomeTitolo = classificazione.getNomeClasse();
//                        }
//                        else if (classificazione.getCodiceCategoria()!= null && !classificazione.getCodiceCategoria().equals("")){
//                            codiceTitolo = classificazione.getCodiceCategoria();
//                            nomeTitolo = classificazione.getNomeCategoria();
//                        }      
//                        
//                        log.debug("Codice titolo: " + codiceTitolo);
//                        log.debug("Nome titolo: " + nomeTitolo);
//                        log.debug("livello: " + fascicolazione.getIdLivelloFascicolo());
//                    }
//                    
//                    
//                    
//                    // ottengo l'oggetto GdDoc completo
////                    for (String idGdDoc : gdDocList) {
////                        
////                        GdDoc gddoc = getGdDocById(idGdDoc);
////                        
////                        // determino se il gddoc è versabile oppure no
////                        isVersabile = isVersabile(gddoc);
////                        
////                        
////                        
////                        
//////                        List<PubblicazioneIoda> pubblicazioni = gddoc.getPubblicazioni();
//////                        for(PubblicazioneIoda p : pubblicazioni){
//////                            log.debug("Numero pubblicazione: " + p.getNumeroPubblicazione());
//////                        }
////                        
//////                        List<Fascicolazione> array = gddoc.getFascicolazioni();
//////                        for (Fascicolazione fascicolazione : array) {
//////                            
//////                            log.debug("Nome fascicolo: " + fascicolazione.getNomeFascicolo());
//////                        }
////                    }
//                    
////                }
////                
////            }
//
//          
//            //        Connection dbConn = null;
////        PreparedStatement ps = null;
////        try {
////            dbConn = UtilityFunctions.getDBConnection();
////            log.debug("connessione db ottenuta");
////
////            dbConn.setAutoCommit(false);
////            log.debug("autoCommit settato a: " + dbConn.getAutoCommit());
////
////
////            
////            // se tutto è ok faccio il commit
////            dbConn.commit();
////        } //try
////        catch (Throwable t) {
////            log.fatal("Errore nel versatore parER", t);
////            try {
////                if (dbConn != null) {
////                    log.info("rollback...");
////                    dbConn.rollback();
////                }
////            }
////            catch (Exception ex) {
////                log.error("errore nel rollback", ex);
////            }
////            throw new JobExecutionException(t);
////        }
////        finally {
////            if (ps != null) {
////                try {
////                    ps.close();
////                } catch (Exception ex) {
////                    log.error(ex);
////                }
////            }
////            if (dbConn != null) {
////                try {
////                    dbConn.close();
////                } catch (Exception ex) {
////                    log.error(ex);
////                }
////            }
//            log.debug("valore canSendPico: " + getCanSendPicoUscita());
//            Thread.sleep(5000);
//        }catch (Throwable t) {
//            log.fatal("Versatore Parer: Errore ...", t);
//        }
//        finally {
//            log.info("Job Versatore Parer finished");
//        }

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
    
//    private String setStartSessioneVersamento(){
//        
//        String res = null;
//                
//        String query = 
//            "INSERT INTO " + ApplicationParams.getSessioniVersamentoParerTableName() + "( " +
//            "id_sessione_versamento_parer, data_inizio) " + 
//            "VALUES (?, date_trunc('sec', now()::timestamp)) ";
//            
//        try (
//            Connection dbConnection = UtilityFunctions.getDBConnection();
//            PreparedStatement ps = dbConnection.prepareStatement(query)
//        ) {         
//            log.debug("setto data e ora inizio sessione di versamento");
//            
//            // ottengo in ID di INDE
//            String idInde = getIndeId();
//            log.debug("valore id INDE: " + idInde);
//            
//            int index = 1;
//            
//            // ID di INDE
//            ps.setString(index++, idInde);
//                                    
//            log.debug("PrepareStatment: " + ps);                      
//            int rowsUpdated = ps.executeUpdate();
//            log.debug("eseguita");
//            if (rowsUpdated == 0)
//                throw new SQLException("data inizio non inserita");
//            else
//                res = idInde;
//        } catch (SQLException | NamingException | IOException | SendHttpMessageException ex) {
//            log.fatal("errore inserimento data_inizio nella tabella sessioni_versamento_parer");
//        }
//        return res;
//    }
    
    private void setStopSessioneVersamento(String idSessioneVersamento){
        
        String res = null;
        
        String query = 
            "UPDATE " + ApplicationParams.getSessioniVersamentoParerTableName() + " " +
            "SET data_fine= date_trunc('sec', now()::timestamp) " + 
            "WHERE id_sessione_versamento_parer = ? ";
            
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(query)
        ) {  
            int index = 1;
            log.debug("setto ora fine sessione di versamento");
            
            ps.setString(index++, idSessioneVersamento);
            
            log.debug("eseguo la query: " + ps.toString() + " ...");
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("Errore inserimento data fine sessione versamento ParER");
            if (result == 0)
                throw new SQLException("data fine non inserita");
        } catch (SQLException | NamingException ex) {
            log.fatal("errore inserimento data_fine nella tabella sessioni_versamento_parer: " + ex);
        }
    }
    
    private SendToParerParams getSendToParerParams(GdDoc gdDoc){
        log.debug("inizio funzione getSendToParerParams");
        
        String idUnitaDocumentaria = null;
        int ordinePresentazione = 1;
        
        // determina se usare id fake dell'unità documentale oppure i reali
        if (getUseFakeId()){
            Random randomGenerator = new Random();
            idUnitaDocumentaria = String.valueOf(randomGenerator.nextInt(100000000));
        }
        else{
            idUnitaDocumentaria = gdDoc.getNumeroRegistrazione();
        }
        
        
        
        
        
        return null;
    }
    
    private ProfiloArchivistico createProfiloArchivistico(GdDoc gdDoc){
        log.debug("creazione del profilo archivistico");
        
        ProfiloArchivistico res = null;
        JSONArray jsonArrayTitoli = null;
        // estrazione del jsonArray con i titoli
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(gdDoc.getDatiParerGdDoc().getXmlSpecifico());
        String str = (String) jsonXmlSpecifico.get("titoli");
        
        if (str != null && !str.equals("")){
            jsonArrayTitoli = (JSONArray) JSONValue.parse(str);
        }
        
        // prendo la prima fascicolazione
        Fascicolazione f = getPrimaFascicolazione(gdDoc.getFascicolazioni());
        ClassificazioneFascicolo c = f.getClassificazione();
        
        String classifica = null;
        
        if (c.getCodiceSottoclasse() != null && !c.getCodiceSottoclasse().equals("")){
            classifica = c.getCodiceSottoclasse().replaceAll("/", ".");
        }
        else if (c.getCodiceClasse() != null && !c.getCodiceClasse().equals("")){
            classifica = c.getCodiceClasse().replaceAll("/", ".");
        }
        else if (c.getCodiceCategoria()!= null && !c.getCodiceCategoria().equals("")){
            classifica = c.getCodiceCategoria().replaceAll("/", ".");
        }
        
        
        log.debug("Codice Fascicolo: " + f.getCodiceFascicolo());
        log.debug("Nome Fascicolo: " + f.getNomeFascicolo());
                                
                    
        log.debug("Codice categoria: " + c.getCodiceCategoria());
        log.debug("Nome categoria: " + c.getNomeCategoria());
        log.debug("Codice classe: " + c.getCodiceClasse());
        log.debug("Nome classe: " + c.getNomeClasse());
        log.debug("Codice sotto-classe: " + c.getCodiceSottoclasse());
        log.debug("Nome sotto-classe: " + c.getNomeSottoclasse());
        
        return null;
    }
    
    private DatiSpecifici getDatiSpecifici(GdDoc gddoc) throws UnsupportedEncodingException{
        
        DatiSpecifici res = null;
        
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(gddoc.getDatiParerGdDoc().getXmlSpecifico());
        
        // estrazione della stringa specifica dal json
        String strDatiSpecifici = (String) jsonXmlSpecifico.get("xmlpart");
        
        //unmarshal nell'oggetto DatiSpecifici
        res = DatiSpecifici.parser(strDatiSpecifici);   
        
        return res;
    }
    
    private String getTipoDocumentoUnitaDocumentaria(GdDoc gddoc, String key){
        
        String res = null;
        
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(gddoc.getDatiParerGdDoc().getXmlSpecifico());
        
        // estrazione della stringa specifica dal json
        String str = (String) jsonXmlSpecifico.get(key);
        
        switch (str) {
            
            case "in":
                res = "DOCUMENTO PROTOCOLLATO IN ENTRATA";
            break;
            
            case "out":
                res = "DOCUMENTO PROTOCOLLATO IN USCITA";
            break;
            
            case "Pico":
                res = "DOCUMENTO PROTOCOLLATO";
            break;
            
            case "Dete":
                res = "DETERMINA";
            break;
         
            case "Deli":
                res = "DELIBERAZIONE";
            break;
            
            case "RegistroRepertorio":
                res = "REGISTRO GIORNALIERO";
            break;
            
            default:
                res = null;
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
    
    private ArrayList<String> getArrayTitoliDocumento(String xmlSpecifico){
        ArrayList<String> res = null;
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(xmlSpecifico);
        JSONArray jsonArrayTitoli = (JSONArray) JSONValue.parse((String) jsonXmlSpecifico.get("titoliDocumento"));
        if (jsonArrayTitoli != null && !jsonArrayTitoli.equals("")){
            res = new ArrayList<>();
            for (int i = 0; i < jsonArrayTitoli.size(); i++) {
                res.add((String) jsonArrayTitoli.get(i));
            }
        }
        return res;
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
            gdDoc.setId(idGdDoc);
        }
        return gdDoc;    
    }
    
    
    private boolean isVersabile(GdDoc gddoc) throws SQLException, NamingException{
        
        boolean res = false;
        Fascicolazione primaFascicolazione = getPrimaFascicolazione(gddoc.getFascicolazioni());
        DateTime dataPrimaFascicolazione = primaFascicolazione.getDataFascicolazione();
        switch (getMovimentazione(gddoc.getDatiParerGdDoc().getXmlSpecifico())) {
            
            case "in":
                // il gddoc è ersabile se sono passati almeno 7 giorni
                Days d = Days.daysBetween(DateTime.now(), gddoc.getDataRegistrazione());
                int giorni = d.getDays();
                
                if (getCanSendPicoEntrata() && giorni > 7){
                    if (dataPrimaFascicolazione != null){
                        replacePlaceholder(gddoc, dataPrimaFascicolazione, null);
                        res = true;
                    }
                }
            break;
            
            case "out":
                if (getCanSendPicoUscita()){
                    if (dataPrimaFascicolazione != null){
                        replacePlaceholder(gddoc, dataPrimaFascicolazione, null);
                        res = true;
                    }
                }
            break;
            
            case "Dete":
                if (getCanSendDete()){
                    PubblicazioneIoda pubblicazione = getEffettivaPubblicazione(gddoc.getPubblicazioni());
                    if (pubblicazione != null && dataPrimaFascicolazione != null){
                        replacePlaceholder(gddoc, dataPrimaFascicolazione, pubblicazione);
                        res = true;
                    }
                }
            break;
            
            case "Deli":
                if (getCanSendDeli()){
                    PubblicazioneIoda pubblicazione = getEffettivaPubblicazione(gddoc.getPubblicazioni());
                    if (pubblicazione != null && dataPrimaFascicolazione != null){
                        replacePlaceholder(gddoc, dataPrimaFascicolazione, pubblicazione);
                        res = true;
                    }
                }
            break;
         
            case "RegistroRepertorio":
                if (getCanSendRegistroGiornaliero()){
                    res = true;
                }
            break;
            
            default:
                res = false;
        }
        return res;
    }
        
    // ripiazza i segna posto con valori reali
    private void replacePlaceholder(GdDoc gddoc, DateTime dataPrimaFascicolazione, PubblicazioneIoda pubblicazione) throws SQLException, NamingException{
        
        // prendo xml specifico
        String xmlSpecifico = gddoc.getDatiParerGdDoc().getXmlSpecifico();
        
        // pattern prescelto per la rappresentazione della data
        String patternData = "yyyy-MM-dd";
        
        // gestione segnaposto pubblicazioni
        if (pubblicazione != null){
          
            xmlSpecifico = xmlSpecifico.replace("[NUMEROPUBBLICAZIONE]", String.valueOf(pubblicazione.getNumeroPubblicazione()));
            xmlSpecifico = xmlSpecifico.replace("[ANNOPUBBLICAZIONE]", String.valueOf(pubblicazione.getAnnoPubblicazione()));
            xmlSpecifico = xmlSpecifico.replace("[INIZIOPUBBLICAZIONE]", toIsoDateFormatString(pubblicazione.getDataDal(), patternData));
            xmlSpecifico = xmlSpecifico.replace("[FINEPUBBLICAZIONE]", toIsoDateFormatString(pubblicazione.getDataAl(), patternData));
            xmlSpecifico = xmlSpecifico.replace("[FINEPUBBLICAZIONE]", toIsoDateFormatString(pubblicazione.getDataAl(), patternData));
            
            if (pubblicazione.getEsecutivita().equalsIgnoreCase("esecutiva")){
                xmlSpecifico = xmlSpecifico.replace("[CONTROLLOREGIONALE]", "NO");
                xmlSpecifico = xmlSpecifico.replace("[DATAESECUTIVITA]", toIsoDateFormatString(pubblicazione.getDataDal(), patternData));
            }
            else{
                xmlSpecifico = xmlSpecifico.replace("[CONTROLLOREGIONALE]", "SI");
            }
            xmlSpecifico = xmlSpecifico.replace("[PUBBESECUTIVITA]", pubblicazione.getEsecutivita());
        }
        
        // gestione segnaposto fascicolazioni
        if (dataPrimaFascicolazione != null){
            xmlSpecifico = xmlSpecifico.replace("[DATAFASCICOLAZIONE]", toIsoDateFormatString(dataPrimaFascicolazione, patternData));
        }
        
        // salvataggio su db
        DatiParerGdDoc dpg = gddoc.getDatiParerGdDoc();      
        dpg.setXmlSpecifico(xmlSpecifico);
        updateDatiParerGdDoc(dpg, gddoc);
    }
    
    
    private void updateDatiParerGdDoc(DatiParerGdDoc datiParer, GdDoc gddoc) throws SQLException, NamingException{
        
        String sqlText = 
                "UPDATE " + ApplicationParams.getDatiParerGdDocTableName() + " SET " +
                "stato_versamento_proposto = coalesce(?, stato_versamento_proposto), " +
                "stato_versamento_effettivo = coalesce(?, stato_versamento_effettivo), " +
                "xml_specifico_parer = coalesce(?, xml_specifico_parer), " +
                "forza_conservazione = coalesce(?, forza_conservazione), " +
                "forza_accettazione = coalesce(?, forza_accettazione), " +
                "forza_collegamento = coalesce(?, forza_collegamento) " +
                "WHERE id_gddoc = ? ";
        
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(sqlText)
        ) {
            int index = 1;
            ps.setString(index++, datiParer.getStatoVersamentoProposto());
            if (datiParer.getStatoVersamentoEffettivo() != null && !datiParer.getStatoVersamentoEffettivo().equals(""))
                ps.setString(index++, datiParer.getStatoVersamentoEffettivo());
            else
                ps.setString(index++, null);
               
            ps.setString(index++, datiParer.getXmlSpecifico());

            // forza_conservazione
            if (datiParer.getForzaConservazione() != null && datiParer.getForzaConservazione())    
                ps.setInt(index++, -1);
            else
                ps.setInt(index++, 0);

            // forza_accettazione
            if (datiParer.getForzaAccettazione()!= null && datiParer.getForzaAccettazione())    
                ps.setInt(index++, -1);
            else
                ps.setInt(index++, 0);

            // forza_collegamento
            if (datiParer.getForzaCollegamento()!= null && datiParer.getForzaCollegamento())    
                ps.setInt(index++, -1);
            else
                ps.setInt(index++, 0);

            ps.setString(index++, gddoc.getId());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int rowsUpdated = ps.executeUpdate();
            log.debug("eseguita");
            if (rowsUpdated == 0)
                throw new SQLException("Documento non trovato");
            else if (rowsUpdated > 1)
                log.fatal("troppe righe aggiornate; aggiornate " + rowsUpdated + " righe, dovrebbe essere una");
        }
        
        
    
            
                

                
            
    }
    
    private Fascicolazione getPrimaFascicolazione(List<Fascicolazione> fascicolazioni){
        
        Fascicolazione res = null;
        
        if (fascicolazioni != null){
            if (fascicolazioni.size() > 0){
                
                // ordino in ordine crescente
                Collections.sort(fascicolazioni);
                
                res = fascicolazioni.get(0);
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
    
    private PubblicazioneIoda getEffettivaPubblicazione(List<PubblicazioneIoda> pubblicazioni){
        
        PubblicazioneIoda res = null;
        
        if (pubblicazioni != null){
            if (pubblicazioni.size() > 0){
                
                // ordino in ordine crescente
                Collections.sort(pubblicazioni);
                
                for (PubblicazioneIoda pubblicazioneIoda : pubblicazioni) {
                    if(pubblicazioneIoda.getAnnoPubblicazione() != null && pubblicazioneIoda.getAnnoPubblicazione() != 0 
                            && pubblicazioneIoda.getNumeroPubblicazione() != null && pubblicazioneIoda.getNumeroPubblicazione() != 0
                            && pubblicazioneIoda.getDataDefissione() == null){
                        res = pubblicazioneIoda;
                        break;
                    }
                }
            }
            else{
                log.debug("pubblicazioni = 0");
            }
        }
        else{
            log.debug("pubblicazioni = null");
        }
        
        return res;
    }
    
    private String toIsoDateFormatString(DateTime dateTime, String pattern){
        DateTimeFormatter dtfOut = DateTimeFormat.forPattern(pattern);
        return dtfOut.print(dateTime);
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
 
    public int getVersione() {
        return versione;
    }

    public void setVersione(int versione) {
        this.versione = versione;
    }
    
    public String getAmbiente() {
        return ambiente;
    }

    public void setAmbiente(String ambiente) {
        this.ambiente = ambiente;
    }
    
    public String getEnte() {
        return ente;
    }

    public void setEnte(String ente) {
        this.ente = ente;
    }
    
    public String getStrutturaVersante() {
        return strutturaVersante;
    }

    public void setStrutturaVersante(String strutturaVersante) {
        this.strutturaVersante = strutturaVersante;
    }
 
    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }
    
    public String getTipoComponenteDefault() {
        return tipoComponenteDefault;
    }

    public void setTipoComponenteDefault(String tipoComponenteDefault) {
        this.tipoComponenteDefault = tipoComponenteDefault;
    }
    
    public String getCodifica() {
        return codifica;
    }

    public void setCodifica(String codifica) {
        this.codifica = codifica;
    }
    
    public boolean getUseFakeId() {
        return useFakeId;
    }

    public void setUseFakeId(boolean useFakeId) {
        this.useFakeId = useFakeId;
    }
    
}
