package it.bologna.ausl.bds_tools.jobs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import groovy.sql.Sql;
import it.bologna.ausl.bds_tools.exceptions.SpedizioniereException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.spedizioniereclient.SpedizioniereAttachment;
import it.bologna.ausl.spedizioniereclient.SpedizioniereClient;
import it.bologna.ausl.spedizioniereclient.SpedizioniereMessage;
import it.bologna.ausl.spedizioniereclient.SpedizioniereRecepit;
import it.bologna.ausl.spedizioniereclient.SpedizioniereRecepit.TipoRicevuta;
import it.bologna.ausl.spedizioniereclient.SpedizioniereStatus;
import it.bologna.ausl.spedizioniereobjectlibrary.Attachment;
import it.bologna.ausl.spedizioniereobjectlibrary.Mail;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.naming.NamingException;
import org.apache.commons.io.IOUtils;
import org.apache.jasper.tagplugins.jstl.core.Catch;
import org.apache.jasper.tagplugins.jstl.core.ForEach;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.xmlbeans.SchemaProperty;
import org.joda.time.Days;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.impl.jdbcjobstore.PostgreSQLDelegate;

/**
 *
 * @author andrea
 */

@DisallowConcurrentExecution
public class Spedizioniere implements Job{
    
    private String connectUri, driver;
    private static final Logger log = LogManager.getLogger(Spedizioniere.class);
    private ExecutorService pool;
    private int maxThread;
    private int timeOutHours;
    private String spedizioniereUrl;
    private String username;
    private String password;
    private int expired;

   
    
    public enum StatiSpedizione {
            DA_INVIARE("da_inviare"),
            PRESA_IN_CARICO("presa_in_carico"),
            ERRORE_PRESA_INCARICO("errore_presa_in_carico"),
            ERRORE("errore"),
            ERRORE_SPEDIZIONE("errore_spedizione"),
            SPEDITO("spedito"),
            CONSEGNATO("consegnato"),
            ERRORE_CONTRLLO_CONSEGNA("errore_controllo_consegna");
            
            private final String key;

            StatiSpedizione(String key) {
                this.key = key;
                //Executors.newFixedThreadPool(i);
            }

            public static Spedizioniere.StatiSpedizione fromString(String key) {
                return key == null
                        ? null
                        : Spedizioniere.StatiSpedizione.valueOf(key.toUpperCase());
            }

            public String getKey() {    
                return key;
            }
            
            @Override
            public String toString() {    
                return getKey();
            }
    }
    
    public enum TipiRicevuta {
            RICEVUTA_ACCETTAZIONE("ricevuta_accettazione"),
            RICEVUTA_CONSEGNA("ricevuta_consegna");
            
            private final String key;

            TipiRicevuta(String key) {
                this.key = key;
                //Executors.newFixedThreadPool(i);
            }

            public static Spedizioniere.TipiRicevuta fromString(String key) {
                return key == null
                        ? null
                        : Spedizioniere.TipiRicevuta.valueOf(key.toUpperCase());
            }

            public String getKey() {    
                return key;
            }
            
            @Override
            public String toString() {    
                return getKey();
            }
    }
    
//    public Spedizioniere() throws SpedizioniereException, SQLException {
//         log.info("Dentro costruttore Spedizioniere");
//        Connection dbConnection = null;
//        try {
//            //log.debug("prendo la connessione...");
//            //dbConnection = UtilityFunctions.getDBConnection();
//            //log.debug("ottenuta connessione");
//            //log.debug("MaxThread= " + maxThread);
////            log.debug("Inizializzo il pool...");
////            pool = Executors.newFixedThreadPool(maxThread);
////            log.debug("pool inizializzato");
//            //pool = null;
//            //log.debug("max thread: " + Integer.valueOf(maxThread));
//            //pool = Executors.newFixedThreadPool(maxThread);
//        }
//        catch(Exception ex) {
//            log.info("Errore nella costruzione dell'oggetto spedizioniere");
//            throw new SpedizioniereException("Errore nella costruzione dell'oggetto spedizioniere", ex);
//        }
//        finally{
//            if (dbConnection != null) {
//                log.debug("dbConnection != null");
//                try {
//                    //dbConnection.close();
//                } catch (Exception ex) {
//                    log.error(ex);
//                }
//            }
//            else{
//                log.debug("dbConnection == null");
//            }
//        }  
//    }
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
    
        log.info("Job Spedizioniere started");
        try {
            log.debug("MaxThread su execute: " + maxThread);
            log.debug("Inizializzo il pool...");
            pool = Executors.newFixedThreadPool(maxThread);
            log.debug("pool inizializzato");
            for(int i = 0; i < maxThread; i++){
                pool.execute(new SpedizioniereThread(i, maxThread));
                pool.shutdown();
                boolean timedOut = !pool.awaitTermination(timeOutHours, TimeUnit.HOURS);
                if (timedOut){
                    throw new SpedizioniereException("timeout");
                }
                else {
                    System.out.println("nothing to do");
                }
            }
        }
        catch (Throwable t) {
            log.fatal("Spedizioniere: Errore ...", t);
        }
        finally {

            log.info("Job Spedizioniere finished");
        }
        
    }
 
    public String getConnectUri() {
        return connectUri;
    }

    public void setConnectUri(String connectUri) {
        this.connectUri = connectUri;
    }

    public String getSpedizioniereUrl() {
        return spedizioniereUrl;
    }

    public void setSpedizioniereUrl(String spedizioniereUrl) {
        this.spedizioniereUrl = spedizioniereUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }
    
     public int getTimeOutHours() {
        return timeOutHours;
    }

    public void setTimeOutHours(int timeOutHours) {
        this.timeOutHours = timeOutHours;
    }

    public int getMaxThread() {
        return maxThread;
    }

    public void setMaxThread(int maxThread) {
        this.maxThread = maxThread;
    }

    public int getExpired() {
        return expired;
    }

    public void setExpired(int expired) {
        this.expired = expired;
    }
    
    private class SpedizioniereThread implements Runnable {
        private final int threadSerial;
        private final int threadsTotal;
        
        public SpedizioniereThread(int threadSerial, int threadsTotal) throws SQLException, NamingException {
            this.threadSerial = threadSerial;
            this.threadsTotal = threadsTotal;
        }

        @Override
        public void run() {
            try {
                log.debug("==============Lancio Spedizione====================");
                spedizione();
                log.debug("==============Fine Spedizione====================");
            }
            catch (Exception ex) {
                log.error(ex);
            }
            try {
                log.debug("=============Lancio ControlloSpedizione============");
                controlloSpedizione();
                log.debug("=============Fine ControlloSpedizione============");
            }
            catch (Exception ex) {
                log.error(ex);
            }
            try {
                log.debug("=============Lancio ControlloConsegna==============");
                controlloConsegna();
                log.debug("=============Fine ControlloConsegna==============");
            }
            catch (Exception ex) {
                log.error(ex);
            }
//            try {
//                gestioneErrore();
//            }
//            catch (Exception ex) {
//                log.error(ex);
//            }  
        }
        
        
        private void spedizione() throws SpedizioniereException {
            String query =  "SELECT " +
                            "id, id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine, id_oggetto, " +
                            "stato, id_applicazione, verifica_timestamp, oggetto_da_spedire_json, " +
                            "utenti_da_notificare, notifica_inviata, numero_errori, da_ritentare " +
                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "WHERE id%? = ? AND (stato = ?::bds_tools.stati_spedizione or stato = ?::bds_tools.stati_spedizione) AND da_ritentare = ? for update";
                            
            try 
                    
                 {
                log.debug("Inizio spedizione()");
                Connection dbConnection = UtilityFunctions.getDBConnection();
                if (dbConnection != null) {
                    log.debug("dbConnection is not null");
                                    }else{
                    log.debug("dbConnction == null");
                }
                PreparedStatement ps = dbConnection.prepareStatement(query);
                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                //ps.setString(3, "da_inviare");
                ps.setString(3, StatiSpedizione.DA_INVIARE.toString());
                //ps.setObject(3, StatiSpedizione.DA_INVIARE);
                ps.setString(4, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                //ps.setString(4, "errore_presa_in_carico");
                ps.setBoolean(5, true);
                
                log.debug("PrepareStatment: " + ps);
                
                ResultSet res = ps.executeQuery();
                
                while (res.next()) {
                    try {
                        if(canSend(res)){
                           spedisci(res); 
                        }
                    }
                    catch (SpedizioniereException ex) {
                        log.debug("ECC_1: " + ex.getMessage());
                        log.error(ex);
                    }
                }
            }
            catch(Exception ex) {
                
                log.debug("ECC_2: " + ex.getMessage());
                throw new SpedizioniereException("Errore nel reperimento dei documenti da spedire", ex);
            }
        }
        
        private void spedisci(ResultSet res) throws SpedizioniereException {
            
            SpedizioniereClient spc = new SpedizioniereClient(getSpedizioniereUrl(), getUsername(), getPassword());
            ArrayList<SpedizioniereAttachment> attachments = new ArrayList<SpedizioniereAttachment>();
            String mongoUri = ApplicationParams.getMongoRepositoryUri();
            MongoWrapper mongo;
            
            try {
                String externalId = res.getString("id_oggetto");
                String oggettoDaSpedire = res.getString("oggetto_da_spedire_json");
                String idOggetto = res.getString("id_oggetto_origine");
                String tipoOggetto =  res.getString("tipo_oggetto_origine");
                int numErrori = res.getInt("numero_errori");
                
                Mail mail = Mail.parse(oggettoDaSpedire);
                int rand = (int)(Math.random()*10);
                SpedizioniereMessage message = new SpedizioniereMessage(mail.getFrom(), mail.getTo(), mail.getCc(), mail.getSubject(), mail.getMessage(), externalId + "_" + rand);
                mongo = new MongoWrapper(mongoUri);
                
                for (Attachment attachment : mail.getAttachments()) {
                    // scaricare da mongo l'inputstream
                    InputStream is = mongo.get(attachment.getUuid());
                    /*
                    File ilFile = new File("c:/tmp/fileTemporaneo.txt");
                    ilFile.deleteOnExit();
                    OutputStream outputStream = new FileOutputStream(ilFile);
                    IOUtils.copy(is, outputStream);
                    outputStream.close();
                    */
                    File tmpFile = File.createTempFile("spedizioniere_", ".tmp");
                    tmpFile.deleteOnExit();
                    OutputStream outputStream = new FileOutputStream(tmpFile);
                    IOUtils.copy(is, outputStream);
                    outputStream.close();
                   SpedizioniereAttachment att = new SpedizioniereAttachment(attachment.getName(), attachment.getMimetype(), tmpFile);
                   attachments.add(att);
                }
                message.setAttachments(attachments);

                
                // Utilizziamo un altro Try Catch per evitare di fare il rollback su tutta la funzione
                try{
                    log.debug("Preparazione spedizione...");
                    String messageId = spc.sendMail(message, false); // forza invio
                    log.debug("spedito al pecgw");
                    log.debug("MessageId: " + messageId);
                    String updateMessageId =    "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET id_spedizione_pecgw=?, stato=?::bds_tools.stati_spedizione " +
                                                "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                    
                    try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(updateMessageId)
                    ) {
                        ps.setInt(1, Integer.valueOf(messageId));
                        ps.setString(2, StatiSpedizione.PRESA_IN_CARICO.toString());
                        ps.setString(3, idOggetto);
                        ps.setString(4, tipoOggetto);
                        ps.executeUpdate();
                    }
                    catch(Exception ex){
                        log.debug("eccezione nell'update di presa_in_carico: " + ex.getMessage());
                    }
                }
                catch(IllegalArgumentException ex){
                    // errori della famiglia 400
                    String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ? and numero_errori=? " +
                                                "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                    
                    try (Connection conn = UtilityFunctions.getDBConnection();) {
                        PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb);
                        ps.setString(1, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                        ps.setBoolean(2, false);
                        ps.setInt(3, numErrori++);
                        ps.setString(4, idOggetto);
                        ps.setString(5, tipoOggetto);

                        ps.executeUpdate();
                    }
                    catch(Exception e){
                        log.debug("eccezione nell'update su eccezione della famiglia 400: " + e.getMessage());
                    }
                }
                catch(Exception ex){
                    String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ? and numero_errori=? " +
                                                "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                    try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb)
                    ) {

                        ps.setString(1, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                        ps.setBoolean(2, true);
                        ps.setInt(3, numErrori++);
                        ps.setString(4, idOggetto);
                        ps.setString(5, tipoOggetto);
                        log.debug("ERRORE PRESA IN CARICO");
                        log.debug("Query: " + ps);
                        ps.executeUpdate();
                    }
                    catch(Exception e){
                        log.debug("eccezione nell'update su eccezione della funzione spedisci(): " + e.getMessage());
                    }
                }
            }
            catch(Exception ex) {
                throw new SpedizioniereException("Errore nella costruzione dell'oggetto mail", ex);
            }
        }
        
        private void controlloSpedizione() throws SpedizioniereException{
            log.debug("ControlloSpedizione avviato!");
            String query = "SELECT " +
                            "id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine " +
                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "WHERE id%? = ? " + 
                            "AND (stato =?::bds_tools.stati_spedizione OR stato =?::bds_tools.stati_spedizione) " +
                            "AND da_ritentare =? FOR UPDATE";
            
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(query)
            ){
                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                ps.setString(3, StatiSpedizione.PRESA_IN_CARICO.toString());
                ps.setString(4, StatiSpedizione.ERRORE_SPEDIZIONE.toString());
                ps.setBoolean(5, true);
                log.debug("Query: " + ps);
                ResultSet res = ps.executeQuery();
                while(res.next()){
                    try {
                        log.debug("Richiamo controllaricevuta()");
                        controllaRicevuta(res, true);
                    } catch (Exception e) {
                    }
                }
            } catch (SQLException | NamingException ex) {
                log.debug(ex.getMessage());
                throw new SpedizioniereException("Errore nel controllo spedizione", ex);
            }
        }
        
        private void controlloConsegna() throws SpedizioniereException{
            String query = "SELECT " +
                            "id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine, verifica_timestamp " +
                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "WHERE id%? = ? AND stato=?::bds_tools.stati_spedizione FOR UPDATE";
            
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(query)
            ){
                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                ps.setString(3, StatiSpedizione.SPEDITO.toString());
                ResultSet res = ps.executeQuery();
                while(res.next()){
                    try {
                        controllaRicevuta(res, false);
                    } catch (Exception e) {
                        log.debug(e);
                    }
                }
            } catch (SQLException | NamingException ex) {
                log.debug(ex);
                throw new SpedizioniereException("Errore nel reperimento delle mail dal DB per controlloConsegna()", ex);
            }
        }
        
        private void gestioneErrore(){}
        
        private void controllaRicevuta(ResultSet res, boolean controlloSpedizione) throws SpedizioniereException{
            log.debug("Dentro controllaRicevuta");
            SpedizioniereClient spc = new SpedizioniereClient(getSpedizioniereUrl(), getUsername(), getPassword());
            
            try {
                String idMsg = String.valueOf(res.getInt("id_spedizione_pecgw"));
                log.debug("richiesta stato mail...");
                SpedizioniereStatus spStatus = spc.getStatus(idMsg);
                log.debug("stato mail ricevuto: " + spStatus.getStatus().name());
                log.debug("richiesta ricevute mail al Pecgw...");
                ArrayList<SpedizioniereRecepit> ricevuteMsg = spc.getRecepits(idMsg); // OTTENGO LE RICEVUTE DAL PECGW  
                log.debug("ottenute le ricevute dal Pecgw");
                int n = ricevuteMsg != null ? ricevuteMsg.size() : 0;
                log.debug("numero di ricevute:" + n);
                
                if (spStatus.getStatus().name().equals("ACCEPTED") || spStatus.getStatus().name().equals("CONFIRMED")) {
                    log.debug("stato ACCEPTED o CONFIRMED");
                    
                    if(ricevuteMsg != null && ricevuteMsg.size() > 0 && controlloSpedizione){ // SE CI SONO RICEVUTE -- SE NON CI SONO CONTROLLO IL TIMESTAMP PER VEDERE SE è UNA MAIL O UNA PEC
                        log.debug("Ricevute != null");
                        
                        String ricevutaFromDb = "SELECT id " +
                            "FROM " + ApplicationParams.getRicevutePecTableName() + " " +
                            "WHERE uuid=?";
                        
                        for (SpedizioniereRecepit spedizioniereRecepit : ricevuteMsg) { // PER OGNI RICEVUTA CONTROLLO SE è PRESENTE NEL DB
                            log.debug("Tipo Ricevuta: " + spedizioniereRecepit.getTipo().getAbbreviation());
                            
                            if (spedizioniereRecepit.getTipo().equals(TipoRicevuta.ERRORE_CONSEGNA.getAbbreviation())
                                    || spedizioniereRecepit.getTipo().equals(TipoRicevuta.NON_ACCETTAZIONE.getAbbreviation())
                                    || spedizioniereRecepit.getTipo().equals(TipoRicevuta.RILEVAZIONE_VIRUS.getAbbreviation())
                                    || spedizioniereRecepit.getTipo().equals(TipoRicevuta.UNKNOWN.getAbbreviation())) {
                                log.debug("Ricevuta con errore");
                                
                                String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                            "SET da_ritentare = ? " +
                                                            "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                                try (
                                    Connection conn = UtilityFunctions.getDBConnection();
                                    PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb)
                                ) {

                                    ps.setBoolean(1, false);
                                    ps.setString(2, res.getString("id_oggetto_origine"));
                                    ps.setString(3, res.getString("tipo_oggetto_origine"));
                                    log.debug("Query: " + ps);
                                    ps.executeUpdate();
                                }
                                catch(Exception e){
                                    log.debug("eccezione nell'update su eccezione della funzione controllaRicevuta(): " + e);
                                }
                            }else{
                                log.debug("Ricevuta senza errore");
                                try (
                                    Connection dbConnection = UtilityFunctions.getDBConnection();
                                    PreparedStatement ps = dbConnection.prepareStatement(ricevutaFromDb)
                                ) {

                                    ps.setString(1, spedizioniereRecepit.getUuid());
                                    log.debug("Query: " + ps);
                                    ResultSet idRicevuta = ps.executeQuery(); // OTTENGO LA RICEVUTA DAL DB

                                    // QUERY CHE SCRIVE SUL DB LA RICEVUTA 
                                    String insertRicevuta = "INSERT INTO " + ApplicationParams.getRicevutePecTableName() + "(" +
                                                            "tipo, uuid, id_oggetto_origine, tipo_oggetto_origine, data_inserimento) " +
                                                            "VALUES (?::bds_tools.tipi_ricevuta, ?, ?, ?, now())";

                                    // SE SUL DB NON è PRESENTE LA RICEVUTA LA SCRIVO
                                    if (!idRicevuta.next()) {
                                        log.debug("idRicevuta == null");
                                        try (
                                            Connection settaUuid = UtilityFunctions.getDBConnection();
                                            PreparedStatement prepared = settaUuid.prepareStatement(insertRicevuta)
                                        ) {
                                            if(spStatus.getStatus().name().equals("ACCEPTED")){
                                                prepared.setString(1, TipiRicevuta.RICEVUTA_ACCETTAZIONE.toString());
                                            }else{
                                                prepared.setString(1, TipiRicevuta.RICEVUTA_CONSEGNA.toString());
                                            }
                                            prepared.setString(2, spedizioniereRecepit.getUuid());
                                            prepared.setString(3, res.getString("id_oggetto_origine"));
                                            prepared.setString(4, res.getString("tipo_oggetto_origine"));
                                            log.debug("Query idRicevuta(INSERT): " + prepared);
                                            prepared.executeUpdate();
                                        } catch (NamingException ex) {
                                            log.debug("Errore: ", ex);
                                            throw new SpedizioniereException("Errore nell'insert delle ricevute", ex);
                                        }
                                    }
                                } catch (NamingException | SpedizioniereException e) {
                                    log.debug(e);
                                    throw new SpedizioniereException("Errore nel reperimento delle ricevute dal DB", e);
                                }
                            }
                        }
                        // QUERY DI AGGIORNAMENTO DEGLI STATI DA CONFIRMED -> CONSEGNATO 
                        //                                    DA ACCEPTED -> PRESA IN CARICO
                        String query = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                    "SET stato=?::bds_tools.stati_spedizione, verifica_timestamp=now() " +
                                                    "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";

                        try (
                            Connection conn = UtilityFunctions.getDBConnection();
                            PreparedStatement ps = conn.prepareStatement(query)
                        ) {
                            if (spStatus.getStatus().name().equals("ACCEPTED")) {
                                ps.setString(1, StatiSpedizione.SPEDITO.toString());
                            }else{
                                ps.setString(1, StatiSpedizione.CONSEGNATO.toString());
                            }
                            ps.setString(2, res.getString("id_oggetto_origine"));
                            ps.setString(3, res.getString("tipo_oggetto_origine"));
                            log.debug("Query: " + ps);
                            ps.executeUpdate();
                        } catch (Exception ex) {
                            log.debug("eccezione aggiornamento stato SPEDITO o CONSEGNATO nella funzione controllaRicevuta()" + ex);
                        }
                    }else{ // NEL CASO SIA UNA MAIL NORMALE E NON UNA PEC DOPO TOT GIORNI DEVE ESSERE MESSA A CONFIMED E SETTARE IL TIMESTAMP
                        log.debug("SONO NELLO STATO CONTROLLO CONSEGNA OPPURE NON HO RICEVUTE");
                        if (spStatus.getStatus().name().equals("ACCEPTED")) {
                            
                            // calcolo i giorni, DIFF TRA IL TIMESTAMP DI E QUELLO SU DB
                            Long ggDiff = getDays(res.getTimestamp("verifica_timestamp"), new Timestamp(new Date().getTime()));
                            log.debug("Giorni differenza: " + ggDiff);
                            if(ggDiff >= getExpired()){ 
                                // QUERY DI AGGIORNAMENTO DEGLI STATI SU DB da CONFIRMED -> CONSEGNATO 
                                String query = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                            "SET stato=?::bds_tools.stati_spedizione, verifica_timestamp=now() " +
                                                            "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";

                                try (
                                    Connection conn = UtilityFunctions.getDBConnection();
                                    PreparedStatement ps = conn.prepareStatement(query)
                                ) {
                                    ps.setString(1, StatiSpedizione.CONSEGNATO.toString());
                                    ps.setString(2, res.getString("id_oggetto_origine"));
                                    ps.setString(3, res.getString("tipo_oggetto_origine"));
                                    log.debug("Query: " + ps);
                                    ps.executeUpdate();
                                } catch (NamingException ex) {
                                    log.debug("Eccezione aggiornamento stato email expired: ", ex);
                                }
                            }else{
                                log.debug("attendo ancora la ricevuta di consegna...");
                            }
                        }
                    } 
                } else if (spStatus.getStatus().name().equals("ERROR")) { // NEL CASO DI STATO DI ERRORE
                    String ricevutaFromDb = "SELECT id " +
                            "FROM " + ApplicationParams.getRicevutePecTableName() + " " +
                            "WHERE uuid=?";
                    
                    for (SpedizioniereRecepit spedizioniereRecepit : ricevuteMsg) { // PER OGNI RICEVUTA CONTROLLO SE è PRESENTE NEL DB
                        try (
                            Connection dbConnection = UtilityFunctions.getDBConnection();
                            PreparedStatement ps = dbConnection.prepareStatement(ricevutaFromDb)
                        ) {
                            ps.setString(1, spedizioniereRecepit.getUuid());

                            ResultSet idRicevuta = ps.executeQuery(); // OTTENGO LA RICEVUTA DAL DB

                            // QUERY CHE SCRIVE SUL DB LA RICEVUTA 
                            String insertRicevuta = "INSERT INTO " + ApplicationParams.getRicevutePecTableName() + "(" +
                                                    "tipo, uuid, id_oggetto_origine, tipo_oggetto_origine, data_inserimento) " +
                                                    "VALUES (?::bds_tools.tipi_ricevuta, ?, ?, ?, ?, now())";

                            // SE SUL DB NON è PRESENTE LA RICEVUTA LA SCRIVO
                            if (idRicevuta == null) {
                                try (
                                    Connection settaUuid = UtilityFunctions.getDBConnection();
                                    PreparedStatement prepared = settaUuid.prepareStatement(insertRicevuta)
                                ) {
                                    if(spStatus.getStatus().name().equals("ACCEPTED")){
                                        prepared.setString(1, TipiRicevuta.RICEVUTA_ACCETTAZIONE.toString());
                                    }else{
                                        prepared.setString(1, TipiRicevuta.RICEVUTA_CONSEGNA.toString());
                                    }
                                    prepared.setString(2, spedizioniereRecepit.getUuid());
                                    prepared.setString(3, res.getString("id_oggetto_origine"));
                                    prepared.setString(4, res.getString("tipo_oggetto_origine"));
                                    prepared.executeUpdate();
                                } catch (NamingException ex) {
                                    log.debug("Errore: ", ex.getMessage());
                                    throw new SpedizioniereException("Errore nell'insert delle ricevute", ex);
                                }
                            }
                        } catch (NamingException e) {
                            log.debug(e);
                            throw new SpedizioniereException("Errore nel reperimento delle ricevute", e);
                        }
                    }
                    
                    String query = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET stato=?::bds_tools.stati_spedizione, verifica_timestamp= now() " +
                                                "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                    try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(query)
                    ) {
                        ps.setString(1, StatiSpedizione.ERRORE.toString());
                        ps.setString(2, res.getString("id_oggetto_origine"));
                        ps.setString(3, res.getString("tipo_oggetto_origine"));
                        log.debug("Query: " + ps);
                        ps.executeUpdate();
                    } catch (NamingException ex) {
                        log.debug("eccezione aggiornamento stato ERRORE nella funzione controllaRicevuta()" + ex);
                    }
                }
            } catch (SQLException | IOException ex) {
                log.debug(ex);
                throw new SpedizioniereException("Errore nel reperimento dello stato o delle ricevute dal Pecgw", ex);
            }
        }
    }
    
    private Boolean canSend(ResultSet res){
        log.debug("canSend started");
        String query = "SELECT attivo " +
                        "FROM " +  ApplicationParams.getSpedizioniereApplicazioni() + " " +
                        "WHERE  id_applicazione_spedizioniere = ? ";              
        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query);
            )  {
            
                String idApplicazione = res.getString("id_applicazione"); 
                ps.setString(1, idApplicazione);
                
                log.debug("Query canSend: " + ps);
                
                ResultSet r = ps.executeQuery();
            if (r.next()) {
                log.debug("attivo == " + r.getInt("attivo"));
                return r.getInt("attivo") == 1 ? true : false ;
            }
        } catch (SQLException | NamingException ex) {
            log.debug("ECCEZIONE: " + ex.getMessage());
            return false;
        }
       return false;
    }
    
    private Long getDays(Timestamp old, Timestamp now){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        
        Date d1 = null;
        Date d2 = null;
        Long diffDays = null;

        try {
            d1 = format.parse(old.toString());
            d2 = format.parse(now.toString());

            //in milliseconds
            long diff = d2.getTime() - d1.getTime();

            diffDays = diff / (24 * 60 * 60 * 1000);

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return diffDays;
    }
    
    private boolean canLaunchControlloConsegna(){
        String queryServizio =  "SELECT nome_servizio, data_inizio, data_fine " +
                        "FROM " + ApplicationParams.getServiziTableName() + " " +
                        "WHERE nome_servizio =?";
        
        String queryRange = "SELECT nome_parametro, val_parametro " +
                            "FROM " + ApplicationParams.getParametriPubbliciTableName() + " " +
                            "WHERE nome_parametro=?";
        
        String queryOraLancio = "SELECT nome_parametro, val_parametro " +
                            "FROM " + ApplicationParams.getParametriPubbliciTableName() + " " +
                            "WHERE nome_parametro=?";
        
        //String rangeNonPermesso = null;
        //String tempList[] = null;
        Timestamp min =  null;
        Timestamp max = null;
        String oraLancio = null;
        String nomeServizio = null;
        Timestamp dataInizio = null;
        Timestamp dataFine = null;
        
        try (
            Connection conn = UtilityFunctions.getDBConnection();
            PreparedStatement ps = conn.prepareStatement(queryRange);
        ) {
            ps.setString(1, "spedizioniereOraControlloConsegnaNonPermesso");
            
            ResultSet res = ps.executeQuery();
            while (res.next()) {
                String rangeNonPermesso = res.getString("val_parametro");  
                String tempList[] = rangeNonPermesso.split(";");
                
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                Date parsedDate = dateFormat.parse(tempList[0]);
                min = new java.sql.Timestamp(parsedDate.getTime());
                parsedDate = dateFormat.parse(tempList[1]);
                max =  new java.sql.Timestamp(parsedDate.getTime());
            }
        }
        catch(Exception ex){
            log.debug("eccezione nella select di range: " + ex);
        }
        
        
        try (
            Connection conn = UtilityFunctions.getDBConnection();
            PreparedStatement ps = conn.prepareStatement(queryOraLancio);
        ) {
            ps.setString(1, "spedizioniereOraControlloConsegna");
            
            ResultSet res = ps.executeQuery();
            while (res.next()) {
                oraLancio = res.getString("val_parametro");   
            }
        }
        catch(Exception ex){
            log.debug("eccezione nella select oraLancio: " + ex);
        }
        
        
        try (
            Connection conn = UtilityFunctions.getDBConnection();
            PreparedStatement ps = conn.prepareStatement(queryRange);
        ) {
            ps.setString(1, "spedizioniere_controllo_consegna");
            
            ResultSet res = ps.executeQuery();
            while (res.next()) {
                nomeServizio = res.getString("nome_servizio"); 
                dataInizio = res.getTimestamp("data_inizio");
                dataFine = res.getTimestamp("data_fine");
            }
        }
        catch(Exception ex){
            log.debug("eccezione nella select param servizio: " + ex);
        }
        
        
        
        return false;
    }
    
}
