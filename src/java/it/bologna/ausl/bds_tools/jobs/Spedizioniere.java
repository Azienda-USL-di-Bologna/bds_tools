package it.bologna.ausl.bds_tools.jobs;

import com.mchange.v2.c3p0.impl.C3P0Defaults;
import it.bologna.ausl.bdm.exception.BdmExeption;
import it.bologna.ausl.bdm.exception.StorageException;
import it.bologna.ausl.bdm.workflows.processes.ProtocolloInUscitaProcess;
import it.bologna.ausl.bdmclient.BdmClient;
import it.bologna.ausl.bdmclient.BdmClientImplementation;
import it.bologna.ausl.bdmclient.BdmClientInterface;
import it.bologna.ausl.bdmclient.RemoteBdmClientImplementation;
import it.bologna.ausl.bds_tools.exceptions.ConvertPdfExeption;
import it.bologna.ausl.bds_tools.exceptions.SpedizioniereException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.masterchefclient.UpdateBabelParams;
import it.bologna.ausl.masterchefclient.WorkerData;
import it.bologna.ausl.masterchefclient.WorkerResponse;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.redis.RedisClient;
import it.bologna.ausl.spedizioniereclient.SpedizioniereAttachment;
import it.bologna.ausl.spedizioniereclient.SpedizioniereClient;
import it.bologna.ausl.spedizioniereclient.SpedizioniereMessage;
import it.bologna.ausl.spedizioniereclient.SpedizioniereRecepit;
import it.bologna.ausl.spedizioniereclient.SpedizioniereRecepit.TipoRicevuta;
import it.bologna.ausl.spedizioniereclient.SpedizioniereStatus;
import it.bologna.ausl.spedizioniereclient.SpedizioniereStatus.Status;
import it.bologna.ausl.spedizioniereobjectlibrary.Attachment;
import it.bologna.ausl.spedizioniereobjectlibrary.Mail;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.naming.NamingException;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */

@DisallowConcurrentExecution
public class Spedizioniere implements Job{
    
    private static final Logger log = LogManager.getLogger(Spedizioniere.class);
    private ExecutorService pool;
    private int maxThread;
    private boolean testMode;
    private int timeOutHours;
    private String spedizioniereUrl;
    private String username;
    private String password;
    private int expired;
    private MetodiSincronizzati metodiSincronizzati;
    private static final String ERRORE_SPEDIZIONE_MAIL = "?CMD=errore_spedizione_mail;";
    private static final String SPEDIZIONIERE_ACTIVE = "isSpedizioniereActive";
   
    
    public enum StatiSpedizione {
            DA_INVIARE("da_inviare"),
            PRESA_IN_CARICO("presa_in_carico"),
            ERRORE_PRESA_INCARICO("errore_presa_in_carico"),
            ERRORE("errore"),
            ERRORE_SPEDIZIONE("errore_spedizione"),
            SPEDITO("spedito"),
            CONSEGNATO("consegnato"),
            ERRORE_CONTRLLO_CONSEGNA("errore_controllo_consegna"),
            ANNULLATO("annullato");
            
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
    
    public enum AvanzamentoProcesso {
            DA_aVANZARE("da_avanzare"),
            AVANZATO("avanzato"),
            ERRORE_AVANZAMENTO("errore_avanzamento"),
            NON_AVANZARE("non_avanzare");
            
            private final String key;

            AvanzamentoProcesso(String key) {
                this.key = key;
                //Executors.newFixedThreadPool(i);
            }

            public static Spedizioniere.AvanzamentoProcesso fromString(String key) {
                return key == null
                        ? null
                        : Spedizioniere.AvanzamentoProcesso.valueOf(key.toUpperCase());
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
    
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
    
        log.info("Job Spedizioniere started");
        
        if (canStartProcess()){
            try {
                metodiSincronizzati = new MetodiSincronizzati();
                log.debug("MaxThread su execute: " + maxThread);
                log.debug("Inizializzo il pool...");
                pool = Executors.newFixedThreadPool(maxThread);
                log.debug("pool inizializzato");

                for(int i = 0; i <= maxThread; i++){ // <= Anzichè < e basta per il thread dello stepOn (Ultimo thread)
                    pool.execute(new SpedizioniereThread(i, maxThread));
                }

                pool.shutdown();
                boolean timedOut = !pool.awaitTermination(timeOutHours, TimeUnit.HOURS);
                if (timedOut){
                    throw new SpedizioniereException("timeout");
                }
                else {
                    System.out.println("nothing to do");
                }
            }
            catch (Throwable t) {
                log.fatal("Spedizioniere: Errore ...", t);
            }
            finally {
                log.info("Job Spedizioniere finished");
            }
        }
        else{
            log.info("Spedizioniere is not running");
            log.info("Job Spedizioniere finished");
        }
        
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

    public boolean isTestMode() {
        return testMode;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
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
            if(threadSerial == maxThread){
                try {
                    stepOn();
                } catch (SQLException ex) {
                   log.error(ex);
                } catch (NamingException ex) {
                    log.error(ex);
                }
            }else{
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
                    if(metodiSincronizzati.check()){
                        controlloConsegna();
                    }
                    log.debug("=============Fine ControlloConsegna==============");
                }
                catch (Exception ex) {
                    log.error(ex);
                }
//                try {
//                    log.debug("=============Lancio GestioneErrore==============");
//                    gestioneErrore();
//                    log.debug("=============Fine GestioneErrore================");
//                }
//                catch (Exception ex) {
//                    log.error(ex);
//                }
            }
  
        }
        
        
        private void spedizione() throws SpedizioniereException {
            String query =  "SELECT " +
                            "id, id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine, id_oggetto, " +
                            "stato, id_applicazione, verifica_timestamp, oggetto_da_spedire_json, " +
                            "utenti_da_notificare, notifica_inviata, numero_errori, da_ritentare, descrizione_oggetto, spedisci_gddoc " +
                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "WHERE id%? = ? AND (stato = ?::bds_tools.stati_spedizione OR (stato = ?::bds_tools.stati_spedizione AND numero_errori >=?)) AND da_ritentare = ? for update";         
            try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query)
            ) {
                log.debug("Inizio spedizione()");
                if (dbConnection != null) {
                    log.debug("dbConnection is not null");
                                    }else{
                    log.debug("dbConnction == null");
                }
                
                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                //ps.setString(3, "da_inviare");
                ps.setString(3, StatiSpedizione.DA_INVIARE.toString());
                //ps.setString(3, StatiSpedizione.DA_INVIARE.toString());
                ps.setString(4, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                ps.setInt(5, expired);
                //ps.setString(4, "errore_presa_in_carico");
                ps.setInt(6, -1);
                
                log.debug("PrepareStatment: " + ps);
                ResultSet res = ps.executeQuery();
                
                while (res.next()) {
                    try {
                        if(canSend(res)){
                           spedisci(res); 
                        }
                    }
                    catch (SpedizioniereException ex) {
                        log.debug("ECC_1: " , ex);
                        log.error(ex);
                    }
                }
            }
            catch(Exception ex) {
                log.debug("ECC_2: " ,ex);
                throw new SpedizioniereException("Errore nel reperimento dei documenti da spedire", ex);
            }
        }
        
        private void spedisci(ResultSet res) throws SpedizioniereException {
            log.debug("Dentro Spedisci()");
            log.debug("Istanzio lo SpedizioniereClient");
            log.debug("Username: " + getUsername());
            log.debug("Password: " + getPassword());
            SpedizioniereClient spc = new SpedizioniereClient(getSpedizioniereUrl(), getUsername(), getPassword());
            ArrayList<SpedizioniereAttachment> attachments = new ArrayList<>();
            log.debug("Richiesta mongoUri");
            String mongoUri = ApplicationParams.getMongoRepositoryUri();
            MongoWrapper mongo;
            
            try {
                log.debug("ID record: " + res.getLong("id"));
                // sarebbe id_destinatario del documento
                String externalId = res.getString("id_oggetto");
                log.debug("Caricamento oggetto_da_spedire_json...");
                String oggettoDaSpedire = res.getString("oggetto_da_spedire_json");
                log.debug("oggetto_da_spedire_json caricato: " + oggettoDaSpedire);
                String idOggetto = res.getString("id_oggetto_origine");
                String tipoOggetto =  res.getString("tipo_oggetto_origine");
                int numErrori = res.getInt("numero_errori");
                
                log.debug("Parse mail");
                Mail mail = Mail.parse(oggettoDaSpedire);
                log.debug("Generate Random externalId");
                int rand = (int)(Math.random()*1000);
                log.debug("Create new SpedizioniereMessage");
                
                log.debug("TestMode: " + testMode);
                log.debug("Expired: " + expired);
                log.debug("MaxThread: " + maxThread);
                log.debug("MaxThread: " + ApplicationParams.getMongoRepositoryUri());
                
                String mittente = null;
                String destinatario = null;
                
                // TEST MODE
                if (testMode) {
                    log.debug("Test mode attiva!");
                    // Contatti di test
                    String queryContattiDiTest =    "SELECT indirizzo " +
                                                    "FROM " + ApplicationParams.getIndirizziMailTestTableName() + ";";
                    try (
                        Connection connContattiDiTest = UtilityFunctions.getDBConnection();
                        PreparedStatement psContattiDiTest = connContattiDiTest.prepareStatement(queryContattiDiTest);
                    ){
                        ResultSet resContattiDiTest = psContattiDiTest.executeQuery();
                        log.debug("Query: " + psContattiDiTest);
                        Boolean mittentePresente = false;
                        Boolean destinatarioPresente = false;
                        log.debug("Mail Originale : " + mail.getTo());
                        while (resContattiDiTest.next()) {
                            if (resContattiDiTest.getString("indirizzo").equals(mail.getTo())) {
                                log.debug("Mail Originale uguale a quella di test (Destinatario)");
                                destinatarioPresente = true;
                                destinatario = mail.getTo();
                            }
                            if (resContattiDiTest.getString("indirizzo").equals(mail.getFrom())) {
                                log.debug("Mail Originale uguale a quella di test (Mittente)");
                                mittentePresente = true;
                                mittente = mail.getFrom();
                            }
                        }
                        log.debug("Fuori dal While Contatti");
                        if (!destinatarioPresente) {
                            destinatario = "babel.care@ausl.bologna.it";
                            log.debug("Destinatario non presente, gli metto: " + destinatario);
                        }
                        if (!mittentePresente) {
                            mittente = "middleware.pec@pec.ausl.bologna.it";
                            log.debug("Mittente non presente, gli metto: " + mittente);
                        }
                        
                    }catch (Exception ex) {
                        log.debug("Errore nel caricamento dei contatti di test: ", ex);
                    }
                }
                
                log.debug("Il mittente è: " + mittente);
                log.debug("Il destinatario è: " + destinatario);
                
                SpedizioniereMessage message = new SpedizioniereMessage(mittente, destinatario, mail.getCc(), mail.getSubject(), mail.getMessage(), externalId + "_" + rand);
                log.debug("new MongoWrapper con URI");
                mongo = new MongoWrapper(mongoUri);
                
                if (mail.getAttachments() != null) {
                    log.debug("Inizio ciclo degli Allegati");
                    for (Attachment attachment : mail.getAttachments()) {
                        log.debug("L'Uuid dell'attachment è: " + attachment.getUuid());
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
                        log.debug("Nome allegato: " + attachment.getName());
                        log.debug("Mimetype allegato: " + attachment.getMimetype());
                        log.debug("Creazione dell'attachment...");
                       SpedizioniereAttachment att = new SpedizioniereAttachment(attachment.getName(), attachment.getMimetype(), tmpFile);
                       log.debug("Attachment creato");
                       attachments.add(att);
                       log.debug("Attachment aggiunto all'arrayList");
                    }
                    log.debug("Fine ciclo degli Allegati");
                }else{
                    log.debug("Mail.getAttachments è uguale a null");
                }
                
                
                if (res.getInt("spedisci_gddoc") != 0) {
                    String prefix = null;
                    String queryApplicazione =  "SELECT prefix " + 
                                                "FROM " + ApplicationParams.getAuthenticationTable() + " " +
                                                "WHERE id_applicazione = ?";
                    try (
                        Connection connForApplicazione = UtilityFunctions.getDBConnection();
                        PreparedStatement psForApplicazione = connForApplicazione.prepareStatement(queryApplicazione)
                    ) {
                        psForApplicazione.setString(1, res.getString("id_applicazione"));
                        log.debug("Caricamento prefix: " + psForApplicazione);
                        ResultSet resOfApplicazione = psForApplicazione.executeQuery();
                        resOfApplicazione.next();
                        prefix = resOfApplicazione.getString("prefix");
                        log.debug("Prefix caricato: " + resOfApplicazione.getString("prefix"));
                    }catch(Exception ex){
                        log.debug("eccezione nel caricamento del prefix dell'applicazione: ", ex);
                    }
                    
                    log.debug("Estrazione gddocs");
                    //Allegati dai sotto documenti
                    String queryGddoc = "SELECT id_gddoc, id_documento_origine, tipo_gddoc, applicazione " +
                                        "FROM " + ApplicationParams.getGdDocsTableName() + " " +
                                        "WHERE id_oggetto_origine = ?";
                    try (
                        Connection connForGddoc = UtilityFunctions.getDBConnection();
                        PreparedStatement psForGddoc = connForGddoc.prepareStatement(queryGddoc)
                    ) {
                        psForGddoc.setString(1, prefix + res.getString("id_oggetto_origine"));
                        log.debug("Query: " + psForGddoc);
                        ResultSet resOfGddoc = psForGddoc.executeQuery();
                        resOfGddoc.next();
                        log.debug("gddoc applicazione in psForGddoc: " + resOfGddoc.getString("applicazione"));
                        
                        if(resOfGddoc.getString("tipo_gddoc").equals("r")){ // Se il gddoc è un record allora carico i suoi sottoDocumenti
                            String querySottoDocumenti =    "SELECT id_sottodocumento, nome_sottodocumento, uuid_mongo_pdf, uuid_mongo_firmato, uuid_mongo_originale, convertibile_pdf, mimetype_file_firmato, mimetype_file_originale, da_spedire_pecgw, spedisci_originale_pecgw " + 
                                                            "FROM " + ApplicationParams.getSottoDocumentiTableName() + " " +
                                                            "WHERE id_gddoc = ?";
                            try (
                                Connection connForSottoDocumenti = UtilityFunctions.getDBConnection();
                                PreparedStatement psForSottoDocumenti = connForSottoDocumenti.prepareStatement(querySottoDocumenti)
                            ) {
                                psForSottoDocumenti.setString(1, resOfGddoc.getString("id_gddoc"));;
                                ResultSet resOfSottoDocumenti = psForSottoDocumenti.executeQuery();

                                while (resOfSottoDocumenti.next()) {
                                    if (resOfSottoDocumenti.getInt("da_spedire_pecgw") != 0) {
                                        InputStream is = null;
                                        String mimeType = null;
                                        Boolean spedisciOriginale = true;
                                        
                                        if (resOfSottoDocumenti.getString("uuid_mongo_firmato") != null) { // FIRMATO
                                            is = mongo.get(resOfSottoDocumenti.getString("uuid_mongo_firmato"));
                                            mimeType = resOfSottoDocumenti.getString("mimetype_file_firmato");
                                        } else if(resOfSottoDocumenti.getInt("convertibile_pdf") != 0){
                                            if(resOfSottoDocumenti.getString("uuid_mongo_pdf") != null){
                                                is = mongo.get(resOfSottoDocumenti.getString("uuid_mongo_pdf"));
                                                mimeType = "application/pdf"; // Da rivedere**********!
                                            }else{
                                                throw new SpedizioniereException("Il sotto documento è convertibile ma uuid_mongo_pdf non è presente");
                                            }
                                        }else{
                                            is = mongo.get(resOfSottoDocumenti.getString("uuid_mongo_originale"));
                                            mimeType = resOfSottoDocumenti.getString("mimetype_file_originale");
                                            spedisciOriginale = false;
                                        }
                                        
                                        if (resOfSottoDocumenti.getInt("spedisci_originale_pecgw") != 0) {
                                            if(spedisciOriginale){
                                                is = mongo.get(resOfSottoDocumenti.getString("uuid_mongo_originale"));
                                                mimeType = resOfSottoDocumenti.getString("mimetype_file_originale");
                                            }
                                        }

                                        File tmpFile = File.createTempFile("spedizioniere_", ".tmp");
                                        tmpFile.deleteOnExit();
                                        OutputStream outputStream = new FileOutputStream(tmpFile);
                                        IOUtils.copy(is, outputStream);
                                        outputStream.close();
                                        SpedizioniereAttachment att = new SpedizioniereAttachment(resOfSottoDocumenti.getString("nome_sottodocumento"), mimeType, tmpFile);
                                        attachments.add(att);
                                    } 
                                }
                            }catch(Exception ex){
                                try {
                                    setMessaggioErrore("Errore nel caricamento del gddoc con id_oggetto_origine: " + res.getString("id_oggetto_origine"), res.getLong("id"));
                                } catch (SQLException e) {
                                    log.debug("Errore update messaggio_errore: ", e);
                                }
                                log.debug("Eccezione nel caricamento del gddoc con id_oggetto_origine: " + res.getString("id_oggetto_origine") + " : " + ex);
                            }
                        }
                    }catch(Exception ex){
                        try {
                            setMessaggioErrore("Errore nel caricamento dei sottodocumenti", res.getLong("id"));
                        } catch (SQLException e) {
                            log.debug("Errore update messaggio_errore caricamento sottodocumenti: ", e);
                        }
                        log.debug("Eccezione nel caricamento del gddoc (sottodocumenti): ", ex);
                    }
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
                        log.debug("eccezione nell'update di presa_in_carico: ", ex);
                    }
                }
                catch(IllegalArgumentException ex){
                    // errori della famiglia 400
                    log.debug("Eccezione nell'ottenimento del messageId dal percgw: " + ex);
                    String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ?, numero_errori=? " +
                                                "WHERE id_oggetto_origine=? AND tipo_oggetto_origine=?";
                    try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb);
                    ) {
                        ps.setString(1, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                        ps.setInt(2, 0);
                        ps.setInt(3, numErrori++);
                        ps.setString(4, idOggetto);
                        ps.setString(5, tipoOggetto);
                        log.debug("Query: " + ps);
                        ps.executeUpdate();
                    }
                    catch(Exception e){
                        log.debug("eccezione nell'update su eccezione della famiglia 400: ", e);
                    }
                } catch(Exception ex){
                    try {
                        setMessaggioErrore("Errore nell'invio della mail", res.getLong("id"));
                    } catch (SQLException e) {
                        log.debug("Errore update messaggio_errore: ", e);
                    }
                    
                    String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ? and numero_errori=? " +
                                                "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                    try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb)
                    ) {
                        ps.setString(1, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                        ps.setInt(2, -1);
                        ps.setInt(3, numErrori++);
                        ps.setString(4, idOggetto);
                        ps.setString(5, tipoOggetto);
                        log.debug("ERRORE PRESA IN CARICO");
                        log.debug("Query: " + ps);
                        ps.executeUpdate();
                    }
                    catch(Exception e){
                        log.debug("eccezione nell'update su eccezione della funzione spedisci(): ", e);
                    }
                }
            }
            catch(Exception ex) {
                try {
                    setMessaggioErrore("Errore nella costruzione della mail", res.getLong("id"));
                } catch (SQLException e) {
                    log.debug("Errore update messaggio_errore: ", e);
                }
                throw new SpedizioniereException("Errore nella costruzione dell'oggetto mail", ex);
            }
        }
        
        private void controlloSpedizione() throws SpedizioniereException{
            log.debug("ControlloSpedizione avviato!");
            String query =  "SELECT " +
                            "id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine, id, descrizione_oggetto " +
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
                ps.setInt(5, -1);
                log.debug("Query: " + ps);
                ResultSet res = ps.executeQuery();
                while(res.next()){
                    try {
                        log.debug("Richiamo controllaricevuta()");
                        controllaRicevuta(res, true);
                    } catch (Exception e) {
                        log.debug(e);
                    }
                }
            } catch (SQLException | NamingException ex) {
                log.debug(ex);
                throw new SpedizioniereException("Errore nel controllo spedizione", ex);
            }
        }
        
        private void controlloConsegna() throws SpedizioniereException{
            String query =  "SELECT " +
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
                metodiSincronizzati.setDataFine();
            } catch (SQLException | NamingException ex) {
                log.debug(ex);
                throw new SpedizioniereException("Errore nel reperimento delle mail dal DB per controlloConsegna()", ex);
            }
        }
        
        private void gestioneErrore(){ 
            log.debug("Dentro gestioneErrore()");
            String query =  "SELECT id, id_oggetto_origine, tipo_oggetto_origine, id_oggetto, stato, id_applicazione, utenti_da_notificare, messaggio_errore " +
                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "WHERE id%? = ? AND ((stato = ?::bds_tools.stati_spedizione " +
                            "AND numero_errori >=?) " +
                            "OR stato = ?::bds_tools.stati_spedizione " +
                            "OR stato = ?::bds_tools.stati_spedizione " +
                            "OR stato = ?::bds_tools.stati_spedizione) " + 
                            "AND notifica_inviata = ?";
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(query)
            ) {
                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                ps.setString(3, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                ps.setInt(4, expired);
                ps.setString(5, StatiSpedizione.ERRORE.toString());
                ps.setString(6, StatiSpedizione.ERRORE_SPEDIZIONE.toString());
                ps.setString(7, StatiSpedizione.ERRORE_CONTRLLO_CONSEGNA.toString());
                ps.setInt(8, 0);
                log.debug("Query: " + ps);
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    if(res.getString("utenti_da_notificare") != null){
                        log.debug("Utente da notificare presente");
                        gestisciErrore(res);
                    }else{
                        log.debug("Utente da notificare non presente");
                    }
                }
            }
            catch(Exception ex){
                log.debug("Eccezione Select gestioneErrore()", ex);     
            }
        }
        
        private void gestisciErrore(ResultSet res) throws Exception{
            log.debug("Dentro gestisciErrore()");
            String nomeApp = null;
            String tokenApp = null;
            
            String query =  "SELECT nome, token " +
                            "FROM " + ApplicationParams.getAuthenticationTable() + " " + 
                            "WHERE  id_applicazione = ?";
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(query)
            ) {
                ps.setString(1, res.getString("id_applicazione"));
                log.debug("Query: " + ps);
                ResultSet r = ps.executeQuery();
                while (r.next()) {
                    nomeApp = r.getString("nome");
                    tokenApp = r.getString("token");
                }
            }
            catch(Exception e){
                log.debug("Eccezione nell'ottenimento del nome dell'applicazione: ",e);
            }
            String queryUrl =   "SELECT url " +
                                "FROM " + ApplicationParams.getSpedizioniereApplicazioni() + " " + 
                                "WHERE  id_applicazione_spedizioniere = ?";
            String url = null;
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(queryUrl)
            ) {
                ps.setString(1, res.getString("id_applicazione"));
                log.debug("QueryURL: " + ps);
                ResultSet r = ps.executeQuery();
                while (r.next()) {
                    url = r.getString("url");
                    log.debug("URL: " + url);
                }
            }
            catch(Exception e){
                log.debug("Eccezione nell'ottenimento dell'url: ", e);
            }
            
            String urlCommand = url + ERRORE_SPEDIZIONE_MAIL; //+ res.getString("id_oggetto_origine");
            UpdateBabelParams updateBabelParams = new UpdateBabelParams(res.getString("id_applicazione"), tokenApp, null, "false", "false", "insert");
            
            String[] utentiDaNotificare = null;
            utentiDaNotificare = res.getString("utenti_da_notificare").split(";");
            
            for (String utente : utentiDaNotificare) {
                updateBabelParams.addAttivita(res.getString("id_oggetto_origine") + "_" + utente, res.getString("id_oggetto_origine"), utente, "3", 
                res.getString("stato"), res.getString("id_oggetto"), null, nomeApp, null, "Apri", urlCommand, null, null, null, null, null, null, null, null, 
                null, null, null, null, res.getString("id_oggetto_origine"), res.getString("tipo_oggetto_origine"), res.getString("id_oggetto_origine"), res.getString("tipo_oggetto_origine"), "group_" + res.getString("id_oggetto_origine"), null);
            }

            String retQueue = "notifica_errore" + "_" + ApplicationParams.getAppId() + "_updateBabelRetQueue_" + ApplicationParams.getServerId();
            log.debug("Creazione Worker..");
            WorkerData wd = new WorkerData(ApplicationParams.getAppId(), "1", retQueue);
            log.debug("Worker creato");
            wd.addNewJob("1", null, updateBabelParams);

//          RedisClient rd = new RedisClient("gdml", null);
//          rd.put(wd.getStringForRedis(), "chefingdml");
            RedisClient rd = new RedisClient(ApplicationParams.getRedisHost(), null);
            log.debug("Add Worker to redis..");
            rd.put(wd.getStringForRedis(), ApplicationParams.getRedisInQueue());
            log.debug("Worker added to redis");

            String extractedValue = rd.bpop(retQueue, 86400);
            WorkerResponse wr = new WorkerResponse(extractedValue);
            
            if (wr.getStatus().equalsIgnoreCase("ok")) {
                log.debug("Worker status: Ok");
                String q =  "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "SET notifica_inviata=? " +
                            "WHERE id = ?";
                try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(q)
                ) {
                    ps.setInt(1, -1);
                    ps.setInt(2, res.getInt("id"));
                    log.debug("Query: " + ps);
                    ps.executeUpdate();
                }
                catch(Exception e){
                    log.debug("Eccezione nella modifica del campo notifica_inviata: ", e);
                }
            }
            else {
                throw new ConvertPdfExeption("errore tornato dal masterchef: " + wr.getError());
//                System.out.println("KO");
            }
        }
        
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
                
                if (spStatus.getStatus() == Status.ACCEPTED || spStatus.getStatus() == Status.CONFIRMED) {
                    log.debug("stato ACCEPTED o CONFIRMED");
                    
                    if(ricevuteMsg != null && ricevuteMsg.size() > 0 && controlloSpedizione){ // SE CI SONO RICEVUTE -- SE NON CI SONO CONTROLLO IL TIMESTAMP PER VEDERE SE è UNA MAIL O UNA PEC
                        log.debug("Ricevute != null");
                        
                        String ricevutaFromDb = "SELECT id " +
                                                "FROM " + ApplicationParams.getRicevutePecTableName() + " " +
                                                "WHERE uuid=?";
                        for (SpedizioniereRecepit spedizioniereRecepit : ricevuteMsg) { // PER OGNI RICEVUTA CONTROLLO SE è PRESENTE NEL DB
                            log.debug("Tipo Ricevuta: " + spedizioniereRecepit.getTipo().getAbbreviation());
                            
                            if (spedizioniereRecepit.getTipo() == TipoRicevuta.ERRORE_CONSEGNA
                                    || spedizioniereRecepit.getTipo() == TipoRicevuta.NON_ACCETTAZIONE
                                    || spedizioniereRecepit.getTipo() == TipoRicevuta.RILEVAZIONE_VIRUS
                                    || spedizioniereRecepit.getTipo() == TipoRicevuta.UNKNOWN) {
                                
                                log.debug("Ricevuta con errore");
                                String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                            "SET da_ritentare = ? " +
                                                            "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                                try (
                                    Connection conn = UtilityFunctions.getDBConnection();
                                    PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb)
                                ) {
                                    ps.setInt(1, 0);
                                    ps.setString(2, res.getString("id_oggetto_origine"));
                                    ps.setString(3, res.getString("tipo_oggetto_origine"));
                                    log.debug("Query: " + ps);
                                    ps.executeUpdate();
                                }
                                catch(Exception e){
                                    log.debug("eccezione nell'update su eccezione della funzione controllaRicevuta(): ", e);
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
                                    String insertRicevuta = "INSERT INTO " + ApplicationParams.getRicevutePecTableName() +
                                                            "(tipo, uuid, id_oggetto_origine, tipo_oggetto_origine, data_inserimento, descrizione, id_spedizione_pec_globale) " +
                                                            "VALUES (?::bds_tools.tipi_ricevuta, ?, ?, ?, now(), ?, ?)";
                                    
                                    // SE SUL DB NON è PRESENTE LA RICEVUTA LA SCRIVO
                                    if (!idRicevuta.next()) {
                                        log.debug("idRicevuta != null");
                                        try (
                                            Connection settaUuid = UtilityFunctions.getDBConnection();
                                            PreparedStatement prepared = settaUuid.prepareStatement(insertRicevuta)
                                        ) {
                                            if(spStatus.getStatus() == Status.ACCEPTED){
                                                prepared.setString(1, TipiRicevuta.RICEVUTA_ACCETTAZIONE.toString());
                                            }else{
                                                prepared.setString(1, TipiRicevuta.RICEVUTA_CONSEGNA.toString());
                                            }
                                            prepared.setString(2, spedizioniereRecepit.getUuid());
                                            prepared.setString(3, res.getString("id_oggetto_origine"));
                                            prepared.setString(4, res.getString("tipo_oggetto_origine"));
                                            prepared.setString(5, res.getString("descrizione_oggetto"));
                                            prepared.setLong(6, res.getLong("id"));
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
                        String query =  "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                        "SET stato=?::bds_tools.stati_spedizione, verifica_timestamp=now() " +
                                        "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                        try (
                            Connection conn = UtilityFunctions.getDBConnection();
                            PreparedStatement ps = conn.prepareStatement(query)
                        ) {
                            if (spStatus.getStatus() == Status.ACCEPTED) {
                                ps.setString(1, StatiSpedizione.SPEDITO.toString());
                            }else{
                                ps.setString(1, StatiSpedizione.CONSEGNATO.toString());
                            }
                            ps.setString(2, res.getString("id_oggetto_origine"));
                            ps.setString(3, res.getString("tipo_oggetto_origine"));
                            log.debug("Query: " + ps);
                            ps.executeUpdate();
                        } catch (Exception ex) {
                            log.debug("eccezione aggiornamento stato SPEDITO o CONSEGNATO nella funzione controllaRicevuta()", ex);
                        }
                    }else{ // NEL CASO SIA UNA MAIL NORMALE E NON UNA PEC DOPO TOT GIORNI DEVE ESSERE MESSA A CONFIMED E SETTARE IL TIMESTAMP
                        log.debug("SONO NELLO STATO CONTROLLO CONSEGNA OPPURE NON HO RICEVUTE");
                        if (spStatus.getStatus() == Status.ACCEPTED) {
                            
                            // calcolo i giorni, DIFF TRA IL TIMESTAMP DI E QUELLO SU DB
                            Long ggDiff = getDays(res.getTimestamp("verifica_timestamp"), new Timestamp(new Date().getTime()));
                            log.debug("Giorni differenza: " + ggDiff);
                            
                            if(ggDiff >= getExpired()){ 
                                // QUERY DI AGGIORNAMENTO DEGLI STATI SU DB da CONFIRMED -> CONSEGNATO 
                                String query =  "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
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
                } else if (spStatus.getStatus() == SpedizioniereStatus.Status.ERROR) { // NEL CASO DI STATO DI ERRORE
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
                            String insertRicevuta = "INSERT INTO " + ApplicationParams.getRicevutePecTableName() +
                                                    "(tipo, uuid, id_oggetto_origine, tipo_oggetto_origine, data_inserimento) " +
                                                    "VALUES (?::bds_tools.tipi_ricevuta, ?, ?, ?, ?, now())";
                            // SE SUL DB NON è PRESENTE LA RICEVUTA LA SCRIVO
                            if (idRicevuta == null) {
                                try (
                                    Connection settaUuid = UtilityFunctions.getDBConnection();
                                    PreparedStatement prepared = settaUuid.prepareStatement(insertRicevuta)
                                ) {
                                    if(spStatus.getStatus() == Status.ACCEPTED){
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
                    
                    String query =  "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
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
                        log.debug("eccezione aggiornamento stato ERRORE nella funzione controllaRicevuta()", ex);
                    }
                }
            } catch (SQLException | IOException ex) {
                try {
                    setMessaggioErrore("Errore nel reperire le ricevute", res.getLong("id"));
                } catch (SQLException e) {
                    log.debug("Errore update messaggio_errore: ", e);
                }
                log.debug(ex);
                throw new SpedizioniereException("Errore nel reperimento dello stato o delle ricevute dal Pecgw", ex);
            }
        }
        
        private void stepOn() throws SQLException, NamingException{
            log.debug("Dentro StepOn");
            String query =  "SELECT id_oggetto_origine " +
                            "FROM ( " + 
                                "SELECT id_oggetto_origine " +
                                "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            ") AS a " +
                            "GROUP BY id_oggetto_origine";
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(query)
            ) {
                log.debug("Query: " + ps);
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    verificaStepOn(res.getString("id_oggetto_origine"));
                }
            }
        }
        
        private void verificaStepOn(String idOggettoOrigine) throws SQLException, NamingException{
            log.debug("Dentro VerificaStepOn");
            String queryVerifica =  "SELECT id_oggetto_origine " +
                                    "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                    "WHERE id_oggetto_origine = ? AND (stato != ?::bds_tools.stati_spedizione AND stato != ?::bds_tools.stati_spedizione)";
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(queryVerifica)
            ) {
                ps.setString(1, idOggettoOrigine);
                ps.setString(2, StatiSpedizione.SPEDITO.toString());
                ps.setString(3, StatiSpedizione.ANNULLATO.toString());
                log.debug("Query: " + ps);
                ResultSet res = ps.executeQuery();
                if(!res.next()){
                    log.debug("Tutte le mail del documento sono in stato spedito");
                    estraiMail(idOggettoOrigine);
                }else{
                    log.debug("Almeno una mail del documento non è in stato spedito");
                }
            }  
        }
        
        private void estraiMail(String idOggettoOrigine){
            log.debug("Dentro EstraMail");
            String query =  "SELECT process_id " +
                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "WHERE id_oggetto_origine = ?";
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(query)
            ) {
                ps.setString(1, idOggettoOrigine);
                log.debug("Query: " + ps);
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    doStepOn(res.getString("process_id"));
                }
            } catch (SQLException ex) {
                log.debug("Errore caricamento mail relative a un documento: ", ex);
            } catch (NamingException ex) {
                log.debug("Errore caricamento mail relative a un documento: ", ex);
            }
        }
        
        private void doStepOn(String processId){
            log.debug("Dentro do Step");
            try {
                BdmClientInterface bdmClient = new RemoteBdmClientImplementation(ApplicationParams.getBdmRestBaseUri());
                if (bdmClient.getCurrentStep(processId).getStepType().equals(ProtocolloInUscitaProcess.Steps.ASPETTA_SPEDIZIONI)) {
                    bdmClient.stepOn(processId, null);
                    String query =  "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                    "SET stato_avanzamento_processo = ?::bds_tools.type_avanzamento_processo " +
                                    "WHERE process_id = ?";
                    try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(query)
                    ) {
                        ps.setString(1, AvanzamentoProcesso.AVANZATO.toString());
                        ps.setString(2, processId);
                        log.debug("Query: " + ps);
                        ps.executeUpdate();
                    } catch (SQLException ex) {
                        log.debug("Errore nel settare lo stato sul db ad avanzato: ", ex);
                    } catch (NamingException ex) {
                        log.debug("Errore nel settare lo stato sul db ad avanzato: ", ex);
                    }
                }
            } catch (StorageException ex) {
                log.debug("Errore nell'instanziare il bdmclient: ", ex);
            } catch (BdmExeption ex) {
                log.debug("Errore nell'instanziare il bdmclient: ", ex);
            }
        }
    }
    
    private void setMessaggioErrore(String messaggio, Long id){
        String queryMessaggioErrore =   "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                        "SET messaggio_errore = ? " +
                                        "WHERE id = ?";
        try (
            Connection connMessaggioErrore = UtilityFunctions.getDBConnection();
            PreparedStatement psMessaggioErrore = connMessaggioErrore.prepareStatement(queryMessaggioErrore)
        ) {
            psMessaggioErrore.setString(1, messaggio);
            psMessaggioErrore.setLong(2, id);
            psMessaggioErrore.executeUpdate();

        } catch (SQLException | NamingException e) {
            log.debug("Errore nell'update del messaggio di errore: ", e);
        }
    }
    
    private Boolean canSend(ResultSet res){
        log.debug("canSend started");
        String query =  "SELECT attivo " +
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
            log.debug("ECCEZIONE: ", ex);
            return false;
        }
       return false;
    }
    
    private Long getDays(Timestamp old, Timestamp now){
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
//        
//        Date d1 = null;
//        Date d2 = null;
//        Long diffDays = null;
//        try {
//            d1 = format.parse(old.toString());
//            d2 = format.parse(now.toString());
//
//            //in milliseconds
//            long diff = d2.getTime() - d1.getTime();
//            diffDays = diff / (24 * 60 * 60 * 1000);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        
        DateTime dtOld = new DateTime(old.getTime());
        DateTime dtNow = new DateTime(now.getTime());
        Duration duration = new Duration(dtOld, dtNow);
        return (duration.getStandardDays());
    }
    
    private java.sql.Timestamp getTimestamp(String data) throws ParseException{
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date parsedDate = dateFormat.parse(data);
        return (new java.sql.Timestamp(parsedDate.getTime()));
    }
    
    private boolean canStartProcess(){
        log.debug("check if Spedizioniere process can start...");
        String query =  "SELECT val_parametro " +
                        "FROM " +  ApplicationParams.getParametriPubbliciTableName() + " " +
                        "WHERE  nome_parametro = ? ";              
        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query);
            )  {
                ps.setString(1, SPEDIZIONIERE_ACTIVE);
                ResultSet r = ps.executeQuery();
            if (r.next()) {
                log.debug("spedizioniere_attivo == " + r.getInt("val_parametro"));
                return r.getInt("val_parametro") != 0 ;
            }
        } catch (SQLException | NamingException ex) {
            log.debug("ECCEZIONE: ", ex);
            return false;
        }
       return false;
    }
 
    public class MetodiSincronizzati {
    
        private  boolean canStartControllo;
        private  boolean haveTocheck;
        //private  final Logger log = LogManager.getLogger(Spedizioniere.class);
        private  int numThread;

        public MetodiSincronizzati(){
            this.canStartControllo = false;
            this.haveTocheck = true;
            this.numThread = 0;
        }

        protected synchronized boolean check() throws SQLException, NamingException, SpedizioniereException{
            log.debug("Dentro Check()");
            // Conto quanti thread ho in totale cosi da poter far settare la data di fine all'ultimo con il setDataFine()
            numThread++;
            if (canStartControllo) { // Il primo thread troverà sempre false questa condizione e sarà costretto a fare il check
                log.debug("Check: First if: return true");
                return true;
            }else if(haveTocheck){ // Il primo thread fa il check poi setto la variabile a false così i successivi thread non lo effettuano
                log.debug("Elese if havetocheck");
                haveTocheck = false;
                String queryRange = "SELECT data_inizio " +
                                    "FROM " + ApplicationParams.getServiziTableName() + " " +
                                    "WHERE nome_servizio=?";

                Timestamp dataInizio = null;

                try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(queryRange);
                ) {
                    ps.setString(1, "spedizioniere_controllo_consegna");

                    ResultSet res = ps.executeQuery();
                    while (res.next()) {
                        dataInizio = res.getTimestamp("data_inizio");
                    }
                }
                catch(Exception ex){
                    log.debug("eccezione nella select param servizio: ", ex);
                }

                if(dataInizio != null){
                    log.debug("dataInizio != null");
                    log.debug("dataInizio = " + dataInizio);
                    Timestamp ora = new Timestamp(new Date().getTime());
                    log.debug("Ora: " + ora);
                    Long giorni =  getDays(dataInizio, ora);
                    log.debug("Controllo consegna giorni differenza: " + giorni);
                    if (giorni != null) {

                        if(giorni >= 1){ // Se il controlloSpedizione() è stato effettuato almeno un giorno fa 
                            setDataInizio();
                            canStartControllo = true; // Setto la variabile a true così i successivi thread partono senza effettuare il check
                            return true; // Ritorno true per far eseguire controlloSpedizione() al primo thread
                        }else{
                            canStartControllo = false;
                            return false;
                        }
                    }
                    throw new SpedizioniereException("Errore nel calcolo nella differenza giorni"); // Sollevo Eccezione perchè la dataInizio è presente 
                                                                                                    // ma si è verificato un errore nel calcolo dei giorni
                }
                else{
                    log.debug("dataInizio == null");
                    // Se la dataInizio è uguale a null la setto e ritorno true per far partire controlloSpedizione() al primo thread 
                    // e setto canControllo per per i successivi
                    setDataInizio();
                    canStartControllo = true;
                    return true;
                }
            }
            return false;
        }

        private void setDataInizio() throws SQLException, NamingException{
            log.debug("Dentro setDataInizio()");
            String query =  "UPDATE " + ApplicationParams.getServiziTableName() + " " +
                            "SET data_inizio=now() " +
                            "WHERE nome_servizio ='spedizioniere_controllo_consegna'";
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(query)
            ) {
                log.debug("Query: " + ps);
                ps.executeUpdate();
            }
        }
    /*
        private Long getDays(Timestamp old, Timestamp now) {
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

            } catch (ParseException ex) {
                log.debug("Eccezione nel calcolo della data: " + ex);
            }

            return diffDays;
        }
    */
        protected synchronized void setDataFine() throws SQLException, NamingException{
            log.debug("Dentro setDataFine()");
            numThread--;
            if (numThread == 0) {
                log.debug("Dentro setDataFine() numThread = 0");
                String query =  "UPDATE " + ApplicationParams.getServiziTableName() + " " +
                                "SET data_fine=now() " +
                                "WHERE nome_servizio ='spedizioniere_controllo_consegna'";
                try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(query)
                ) {
                    log.debug("Query: " + ps);
                    ps.executeUpdate();
                }
            }
        }
    }
    
}
