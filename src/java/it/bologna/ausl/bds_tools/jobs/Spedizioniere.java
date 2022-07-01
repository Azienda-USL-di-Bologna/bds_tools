package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bdm.core.Bdm;
import it.bologna.ausl.bdm.core.Step;
import it.bologna.ausl.bdm.utilities.Bag;
import it.bologna.ausl.bdm.workflows.processes.ProtocolloInUscitaProcess;
import it.bologna.ausl.bdmclient.BdmClientInterface;
import it.bologna.ausl.bdmclient.RemoteBdmClientImplementation;
import it.bologna.ausl.bds_tools.MailSender;
import it.bologna.ausl.bds_tools.exceptions.ConvertPdfExeption;
import it.bologna.ausl.bds_tools.exceptions.DocumentNotReadyException;
import it.bologna.ausl.bds_tools.exceptions.SpedizioniereException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.ConfigParams;
import it.bologna.ausl.bds_tools.utils.SupportedFile;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.mail.MessagingException;
import javax.naming.NamingException;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.json.simple.JSONObject;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */
@DisallowConcurrentExecution
public class Spedizioniere implements Job {

    private static final Logger log = LogManager.getLogger("spedizioniere-logger");
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
    private static volatile Set<String> documentiIncriminati = new HashSet<>(); // Sarà popolato ogni volta che verrà rilevato un documento con la lettera non firmata
    private static final String ERRORE_LETTERA_NON_FIRMATA = "ERRORE_LETTERA_NON_FIRMATA";
    private static final String ERRORE_COSTRUZIONE_MAIL = "ERRORE_COSTRUZIONE_MAIL";

    public enum StatiSpedizione {
        DA_INVIARE("da_inviare"),
        PRESA_IN_CARICO("presa_in_carico"),
        ERRORE_PRESA_INCARICO("errore_presa_in_carico"),
        ERRORE_CONSEGNA("errore_consegna"),
        ERRORE_SPEDIZIONE("errore_spedizione"),
        SPEDITO("spedito"),
        INVIATO("inviato"),
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

    public enum StatiAvanzamentoProcesso {
        DA_AVANZARE("da_avanzare"),
        AVANZATO("avanzato"),
        ERRORE_AVANZAMENTO("errore_avanzamento"),
        NON_AVANZARE("non_avanzare");

        private final String key;

        StatiAvanzamentoProcesso(String key) {
            this.key = key;
            //Executors.newFixedThreadPool(i);
        }

        public static Spedizioniere.StatiAvanzamentoProcesso fromString(String key) {
            return key == null
                    ? null
                    : Spedizioniere.StatiAvanzamentoProcesso.valueOf(key.toUpperCase());
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

//        if(true)
//            return;
        log.info("Job Spedizioniere started");

        if (canStartProcess()) {
            try {
                metodiSincronizzati = new MetodiSincronizzati();
                log.info("MaxThread su execute: " + maxThread);
                log.info("Inizializzo il pool...");
                pool = Executors.newFixedThreadPool(maxThread);
                log.info("pool inizializzato");

                for (int i = 0; i <= maxThread; i++) { // <= Anzichè < e basta per il thread dello stepOn (Ultimo thread)
                    pool.execute(new SpedizioniereThread(i, maxThread));
                }

                pool.shutdown();
                boolean timedOut = !pool.awaitTermination(timeOutHours, TimeUnit.HOURS);
                if (timedOut) {
                    throw new SpedizioniereException("timeout");
                } else {
                    System.out.println("nothing to do");
                }
            } catch (Throwable t) {
                log.fatal("Spedizioniere: Errore ...", t);
            } finally {
                log.info("Job Spedizioniere finished");
            }
        } else {
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
            if (threadSerial == maxThread) {
                try {
                    stepOn();
                } catch (SQLException ex) {
                    log.error(ex);
                } catch (NamingException ex) {
                    log.error(ex);
                }
            } else {
                try {
                    log.info("==============Lancio Spedizione====================");
                    spedizione();
                    log.info("==============Fine Spedizione====================");
                } catch (Exception ex) {
                    log.error(ex);
                }
                try {
                    log.info("=============Lancio ControlloSpedizione============");
                    controlloSpedizione();
                    log.info("=============Fine ControlloSpedizione============");
                } catch (Exception ex) {
                    log.error(ex);
                }
                try {
                    log.info("=============Lancio ControlloConsegna==============");
                    if (metodiSincronizzati.check()) {
                        log.info("Controllo consegna starting now...");
                        controlloConsegna();
                    }
                    log.info("=============Fine ControlloConsegna==============");
                } catch (Exception ex) {
                    log.error(ex);
                }
                try {
                    log.info("=============Lancio GestioneErrore==============");
                    gestioneErrore();
                    log.info("=============Fine GestioneErrore================");
                } catch (Exception ex) {
                    log.error(ex);
                }
            }

        }

        private void spedizione() throws SpedizioniereException {
            String query = "SELECT "
                    + "id, id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine, id_oggetto, "
                    + "stato, id_applicazione, verifica_timestamp, oggetto_da_spedire_json, "
                    + "utenti_da_notificare, notifica_inviata, numero_errori, da_ritentare, descrizione_oggetto, spedisci_gddoc "
                    + "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                    + "WHERE id%? = ? AND (stato = ?::bds_tools.stati_spedizione OR (stato = ?::bds_tools.stati_spedizione AND numero_errori >=?)) AND da_ritentare = ? for update";
            try (
                    Connection dbConnection = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = dbConnection.prepareStatement(query)) {
                log.info("Inizio spedizione()");
                if (dbConnection != null) {
                    log.info("dbConnection is not null");
                } else {
                    log.info("dbConnction == null");
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

                log.info("PrepareStatment: " + ps);
                ResultSet res = ps.executeQuery();

                while (res.next()) {
                    try {
                        if (canSend(res)) {
                            spedisci(res);
                        }
                    } catch (SpedizioniereException ex) {
                        log.error("ECC_1: ", ex);
                        log.error(ex);
                    }
                }
            } catch (Exception ex) {
                log.error("ECC_2: ", ex);
                throw new SpedizioniereException("Errore nel reperimento dei documenti da spedire", ex);
            }
        }

        private void spedisci(ResultSet res) throws SpedizioniereException, NamingException {
            log.info("Dentro Spedisci()");
            log.info("Istanzio lo SpedizioniereClient");
            log.info("Username: " + getUsername());
            log.info("Password: " + getPassword());
            SpedizioniereClient spc = new SpedizioniereClient(getSpedizioniereUrl(), getUsername(), getPassword());
            ArrayList<SpedizioniereAttachment> attachments = new ArrayList<>();
            ArrayList<File> tempFiles = new ArrayList<>();
            log.info("Richiesta mongoUri");
            String mongoUri = ApplicationParams.getMongoRepositoryUri();
            MongoWrapper mongo;

            try {
                long id = res.getLong("id");
                log.info("ID record: " + id);
                // sarebbe id_destinatario del documento
                String externalId = res.getString("id_oggetto");
                log.info("Caricamento oggetto_da_spedire_json...");
                String oggettoDaSpedire = res.getString("oggetto_da_spedire_json");
                log.info("oggetto_da_spedire_json caricato: " + oggettoDaSpedire);

                String idOggetto = res.getString("id_oggetto_origine");
                String tipoOggetto = res.getString("tipo_oggetto_origine");
                int numErrori = res.getInt("numero_errori");

                log.info("Parse mail");
                Mail mail = Mail.parse(oggettoDaSpedire);
                log.info("Generate Random externalId");
                int rand = (int) (Math.random() * 1000);
                log.info("Create new SpedizioniereMessage");

                log.info("TestMode: " + testMode);
                log.info("Expired: " + expired);
                log.info("MaxThread: " + maxThread);
                log.info("MongoRepository: " + ApplicationParams.getMongoRepositoryUri());

                String mittente = mail.getFrom();
                String destinatario = mail.getTo();

                // TEST MODE
                if (testMode) {
                    log.info("Test mode attiva!");
                    // Contatti di test
                    String queryContattiDiTest = "SELECT indirizzo "
                            + "FROM " + ApplicationParams.getIndirizziMailTestTableName() + ";";
                    try (
                            Connection connContattiDiTest = UtilityFunctions.getDBConnection();
                            PreparedStatement psContattiDiTest = connContattiDiTest.prepareStatement(queryContattiDiTest);) {
                        ResultSet resContattiDiTest = psContattiDiTest.executeQuery();
                        log.info("Query: " + psContattiDiTest);
                        Boolean mittentePresente = false;
                        Boolean destinatarioPresente = false;
                        log.info("Mail Originale : " + destinatario);
                        while (resContattiDiTest.next()) {
                            if (resContattiDiTest.getString("indirizzo").equals(destinatario)) {
                                log.info("Mail Originale uguale a quella di test (Destinatario)");
                                destinatarioPresente = true;
//                                destinatario = mail.getTo();
                            }
                            if (resContattiDiTest.getString("indirizzo").equals(mittente)) {
                                log.info("Mail Originale uguale a quella di test (Mittente)");
                                mittentePresente = true;
//                                mittente = mail.getFrom();
                            }
                        }
                        log.info("Fuori dal While Contatti");
                        if (!destinatarioPresente) {
                            destinatario = ApplicationParams.getSystemPecMail();
//                            destinatario = "babel.care@ausl.bologna.it";
                            log.info("Destinatario non presente, gli metto: " + destinatario);
                        }
                        if (!mittentePresente) {
                            mittente = ApplicationParams.getSystemPecMail();
                            log.info("Mittente non presente, gli metto: " + mittente);
                        }

                    } catch (Exception ex) {
                        log.info("Errore nel caricamento dei contatti di test: ", ex);
                    }
                }

                log.info("Il mittente è: " + mittente);
                log.info("Il destinatario è: " + destinatario);

                SpedizioniereMessage message = new SpedizioniereMessage(mittente, destinatario, mail.getCc(), mail.getSubject(), mail.getMessage(), externalId + "_" + rand);
                log.info("new MongoWrapper con URI");
                boolean useMinIO = ApplicationParams.getUseMinIO();
                JSONObject minIOConfig = ApplicationParams.getMinIOConfig();
                String codiceAzienda = ApplicationParams.getCodiceAzienda();
                Integer maxPoolSize = Integer.parseInt(minIOConfig.get("maxPoolSize").toString());
                mongo = MongoWrapper.getWrapper(
                        useMinIO,
                        mongoUri,
                        minIOConfig.get("DBDriver").toString(),
                        minIOConfig.get("DBUrl").toString(),
                        minIOConfig.get("DBUsername").toString(),
                        minIOConfig.get("DBPassword").toString(),
                        codiceAzienda,
                        maxPoolSize,
                        null);

                if (mail.getAttachments() != null) {
                    log.info("Inizio ciclo degli Allegati");
                    for (Attachment attachment : mail.getAttachments()) {
                        log.info("L'Uuid dell'attachment è: " + attachment.getUuid());
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
                        tempFiles.add(tmpFile);
                        //tmpFile.deleteOnExit();
                        try (OutputStream outputStream = new FileOutputStream(tmpFile)) {
                            IOUtils.copy(is, outputStream);
                        } finally {
                            is.close();
                        }
                        log.info("Nome allegato: " + attachment.getName());
                        log.info("Mimetype allegato: " + attachment.getMimetype());
                        log.info("Creazione dell'attachment...");
                        SpedizioniereAttachment att = new SpedizioniereAttachment(attachment.getName(), attachment.getMimetype(), tmpFile);
                        log.info("Attachment creato");
                        attachments.add(att);
                        log.info("Attachment aggiunto all'arrayList");
                    }
                    log.info("Fine ciclo degli Allegati");
                } else {
                    log.info("Mail.getAttachments è uguale a null");
                }

                if (res.getInt("spedisci_gddoc") != 0) {
                    log.info("Dentro spedisci gddoc");
                    log.info("Estrazione gddocs");
                    //Allegati dai sotto documenti
                    String queryGddoc = "SELECT d.id_gddoc, d.id_documento_origine, d.tipo_gddoc, d.applicazione "
                            + "FROM " + ApplicationParams.getGdDocsTableName() + " d JOIN "
                            + ApplicationParams.getAuthenticationTable() + " a ON d.applicazione = a.id_applicazione "
                            + "WHERE id_oggetto_origine = a.prefix || ? AND a.id_applicazione = ?";
                    try (
                            Connection connForGddoc = UtilityFunctions.getDBConnection();
                            PreparedStatement psForGddoc = connForGddoc.prepareStatement(queryGddoc)) {
                        psForGddoc.setString(1, res.getString("id_oggetto_origine"));
                        psForGddoc.setString(2, res.getString("id_applicazione"));
                        log.info("Query: " + psForGddoc);
                        ResultSet resOfGddoc = psForGddoc.executeQuery();
                        while (resOfGddoc.next()) {
                            log.info("gddoc applicazione in psForGddoc: " + resOfGddoc.getString("applicazione"));
                            if (resOfGddoc.getString("tipo_gddoc").equals("r")) { // Se il gddoc è un record allora carico i suoi sottoDocumenti
                                log.info("Il gddoc è di tipo r");
                                String querySottoDocumenti = "SELECT id_sottodocumento, nome_sottodocumento, uuid_mongo_pdf, uuid_mongo_firmato, "
                                        + "uuid_mongo_originale, convertibile_pdf, mimetype_file_firmato, mimetype_file_originale, "
                                        + "da_spedire_pecgw, spedisci_originale_pecgw, tipo_sottodocumento "
                                        + "FROM " + ApplicationParams.getSottoDocumentiTableName() + " "
                                        + "WHERE id_gddoc = ?";
                                try (
                                        Connection connForSottoDocumenti = UtilityFunctions.getDBConnection();
                                        PreparedStatement psForSottoDocumenti = connForSottoDocumenti.prepareStatement(querySottoDocumenti)) {
                                    psForSottoDocumenti.setString(1, resOfGddoc.getString("id_gddoc"));;
                                    ResultSet resOfSottoDocumenti = psForSottoDocumenti.executeQuery();

                                    while (resOfSottoDocumenti.next()) {
                                        log.info("Sottodocumento: " + resOfSottoDocumenti.getString("id_sottodocumento"));
                                        if (resOfSottoDocumenti.getInt("da_spedire_pecgw") != 0) {
                                            InputStream is = null;
                                            String mimeType = null;
                                            Boolean spedisciOriginale = true;

                                            if (resOfSottoDocumenti.getString("uuid_mongo_firmato") != null) { // FIRMATO
                                                log.info("Sottodocumento firmato");
                                                is = mongo.get(resOfSottoDocumenti.getString("uuid_mongo_firmato"));
                                                mimeType = resOfSottoDocumenti.getString("mimetype_file_firmato");
                                            } else if (resOfSottoDocumenti.getString("uuid_mongo_firmato") == null && resOfSottoDocumenti.getString("tipo_sottodocumento").equals("testo")) { // Lettera firmata non presente allertare babelAlert
                                                log.info("Lettera non firmata!");
                                                if (!documentiIncriminati.contains(res.getString("id_oggetto_origine"))) {
//                                                    // Metadata
//                                                    String[] to = new String[1];
//                                                    if (ConfigParams.getAmbiente().toLowerCase().equals("test")) {  // Se in ambiente di test spedisci solo a Fayssel
//                                                        to[0] = "f.sabir@nextsw.it";
//                                                    }else{
//                                                        to[0] = ApplicationParams.getOtherPublicParam("EmailsAddressesAlert");
//                                                    }
//                                                    String from = ApplicationParams.getOtherPublicParam("EmailsAlertFrom");
//                                                    String host = ApplicationParams.getOtherPublicParam("mailServerSmtpUrl");
//                                                    int port = Integer.valueOf(ApplicationParams.getOtherPublicParam("mailServerSmtpPort"));
//                                                    // Email
                                                    String subject = "SPEDIZIONIERE | " + ConfigParams.getAmbiente() + "/" + ConfigParams.getAzienda() + " - Tentativo di invio documento con lettera non firmata";
                                                    writeDiagnoticaReport(res, ERRORE_LETTERA_NON_FIRMATA, new SpedizioniereException("Documento con lettera non firmata!"));
                                                    // sendMail(res, subject, new SpedizioniereException("Documento con lettera non firmata!"));
//                                                    String messageBody = "Dati del documento:<br/>";
//                                                    messageBody += "id: " + res.getString("id") +"<br/>";
//                                                    messageBody += "id_oggetto_origine: " + res.getString("id_oggetto_origine") + "<br/>";
//                                                    messageBody += "tipo_oggetto_origine: " + res.getString("tipo_oggetto_origine") + "<br/>";
//                                                    messageBody += "id_applicazione: " + res.getString("id_applicazione") + "<br/>";
//                                                    messageBody += "Azienda: " + ConfigParams.getAzienda() + "<br/>";
//                                                    messageBody += "Ambiente: " + ConfigParams.getAmbiente();
//                                                    MailSender mailSender = new MailSender(to, from, host, subject, messageBody, port);
//                                                    try {
//                                                        mailSender.send();
//                                                    } catch (Exception e) {
//                                                        log.info("Errore nell'invio della mail a babelAlert!");
//                                                        throw new SpedizioniereException("Documento con lettera non firmata!");
//                                                    }
                                                    documentiIncriminati.add(res.getString("id_oggetto_origine"));
                                                }
                                                throw new SpedizioniereException("Documento con lettera non firmata! I membri di babelAlert sono stati avvisati via mail.");
                                            } else if (resOfSottoDocumenti.getInt("convertibile_pdf") != 0) {
                                                if (resOfSottoDocumenti.getString("uuid_mongo_pdf") != null) {
                                                    log.info("Sottodocumento convertibile quindi pdf");
                                                    is = mongo.get(resOfSottoDocumenti.getString("uuid_mongo_pdf"));
                                                    mimeType = "application/pdf"; // Da rivedere**********!
                                                } else {
                                                    throw new SpedizioniereException("Il sotto documento è convertibile ma uuid_mongo_pdf non è presente");
                                                }
                                            } else {
                                                log.info("Sottodocumento originale else");
                                                is = mongo.get(resOfSottoDocumenti.getString("uuid_mongo_originale"));
                                                mimeType = resOfSottoDocumenti.getString("mimetype_file_originale");
                                                spedisciOriginale = false;
                                            }

                                            log.info("Creazione tmpFile");
                                            File tmpFile = File.createTempFile("spedizioniere_", ".tmp");
                                            tempFiles.add(tmpFile);
                                            //tmpFile.deleteOnExit();
                                            try (OutputStream outputStream = new FileOutputStream(tmpFile)) {
                                                IOUtils.copy(is, outputStream);
                                            } finally {
                                                is.close();
                                            }
                                            log.info("mimeType: " + mimeType);
                                            log.info("calcolo estensione...");
                                            String ext = SupportedFile.getSupportedFile(ApplicationParams.getSupportedFileList(), mimeType).getExtension().toLowerCase();
                                            log.info("estensione: " + ext);
                                            log.info("Creazione attachment");
                                            SpedizioniereAttachment att = new SpedizioniereAttachment(resOfSottoDocumenti.getString("nome_sottodocumento") + "." + ext, mimeType, tmpFile);
                                            log.info("Aggiunta attachment");
                                            attachments.add(att);

                                            if (resOfSottoDocumenti.getInt("spedisci_originale_pecgw") != 0) {
                                                if (spedisciOriginale) {
                                                    log.info("Sottodocumento originale");
                                                    is = mongo.get(resOfSottoDocumenti.getString("uuid_mongo_originale"));
                                                    mimeType = resOfSottoDocumenti.getString("mimetype_file_originale");

                                                    log.info("Creazione tmpFile");
                                                    tmpFile = File.createTempFile("spedizioniere_", ".tmp");
                                                    tempFiles.add(tmpFile);
                                                    //tmpFile.deleteOnExit();
                                                    try (OutputStream outputStream = new FileOutputStream(tmpFile)) {
                                                        IOUtils.copy(is, outputStream);
                                                    } catch (IOException ex) {
                                                        log.info("Eccezione nel creare file temporaneo per l'attachment", ex);
                                                    } finally {
                                                        is.close();
                                                    }
                                                    log.info("mimeType: " + mimeType);
                                                    log.info("calcolo estensione...");
                                                    ext = SupportedFile.getSupportedFile(ApplicationParams.getSupportedFileList(), mimeType).getExtension().toLowerCase();
                                                    log.info("estensione: " + ext);
                                                    log.info("Creazione attachment");
                                                    att = new SpedizioniereAttachment(resOfSottoDocumenti.getString("nome_sottodocumento") + "." + ext, mimeType, tmpFile);
                                                    log.info("Aggiunta attachment");
                                                    attachments.add(att);
                                                }
                                            }
                                        }
                                    }
                                } catch (SQLException | NamingException ex) {
                                    try {
                                        setMessaggioErrore("Errore nel caricamento del gddoc con id_oggetto_origine: " + res.getString("id_oggetto_origine"), res.getLong("id"));
                                    } catch (SQLException e) {
                                        log.info("Errore update messaggio_errore: ", e);
                                    }
                                    log.info("Eccezione nel caricamento del gddoc con id_oggetto_origine: " + res.getString("id_oggetto_origine") + " : " + ex);
                                }
                            } else {
                                throw new DocumentNotReadyException("Il gddoc non è ancora un record");
                            }
                        }

                    } catch (DocumentNotReadyException ex) {
                        log.info("il documento non è stato preso in carico perché non è ancora un record", ex);
                        return;
                    } catch (SQLException | NamingException ex) {
                        log.info("Eccezione nel caricamento del gddoc (sottodocumenti): ", ex);
                        try {
                            setMessaggioErrore("Errore nel caricamento dei sottodocumenti", res.getLong("id"));
                        } catch (SQLException e) {
                            log.info("Errore update messaggio_errore caricamento sottodocumenti: ", e);
                        }
                        throw ex;
                    }
                }

                message.setAttachments(attachments);

                // Utilizziamo un altro Try Catch per evitare di fare il rollback su tutta la funzione
                try {
                    log.info("Preparazione spedizione...");
                    String messageId = spc.sendMail(message, false); // forza invio
                    log.info("spedito al pecgw");
                    log.info("MessageId: " + messageId);
                    String updateMessageId = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                            + "SET id_spedizione_pecgw=?, stato=?::bds_tools.stati_spedizione "
                            + "WHERE id = ?";
                    try (
                            Connection conn = UtilityFunctions.getDBConnection();
                            PreparedStatement ps = conn.prepareStatement(updateMessageId)) {
                        //ps.setInt(1, Integer.valueOf(messageId));
                        ps.setString(1, messageId);
                        ps.setString(2, StatiSpedizione.PRESA_IN_CARICO.toString());
                        ps.setLong(3, id);
                        ps.executeUpdate();
                    } catch (Exception ex) {
                        log.info("eccezione nell'update di presa_in_carico: ", ex);
                    }
                } catch (IllegalArgumentException ex) {
                    // errori della famiglia 400
                    log.info("Eccezione nell'ottenimento del messageId dal percgw: " + ex);
                    String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                            + "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ?, numero_errori=?, verifica_timestamp=now() "
                            + "WHERE id = ?";
                    try (
                            Connection conn = UtilityFunctions.getDBConnection();
                            PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb);) {
                        ps.setString(1, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                        ps.setInt(2, 0);
                        ps.setInt(3, numErrori++);
                        ps.setLong(4, id);
                        log.info("Query: " + ps);
                        ps.executeUpdate();
                        gestisciErrore(res);
                    } catch (Exception e) {
                        log.info("eccezione nell'update su eccezione della famiglia 400: ", e);
                    }
                } catch (Exception ex) {
                    try {
                        setMessaggioErrore("Errore nell'invio della mail", res.getLong("id"));
                    } catch (SQLException e) {
                        log.info("Errore update messaggio_errore: ", e);
                    }

                    String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                            + "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ?, numero_errori=? "
                            + "WHERE id = ?";
                    try (
                            Connection conn = UtilityFunctions.getDBConnection();
                            PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb)) {
                        ps.setString(1, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                        ps.setInt(2, -1);
                        ps.setInt(3, numErrori++);
                        ps.setLong(4, id);
                        log.info("ERRORE PRESA IN CARICO");
                        log.info("Query: " + ps);
                        ps.executeUpdate();
                    } catch (Exception e) {
                        log.info("eccezione nell'update su eccezione della funzione spedisci(): ", e);
                    }
                } finally {
                    for (SpedizioniereAttachment attachment : attachments) {
                        attachment.getFile().delete();
                    }
                }
            } catch (Exception ex) {

                try {
                    setMessaggioErrore("Errore nella costruzione della mail", res.getLong("id"));
                    if (!documentiIncriminati.contains(res.getString("id_oggetto_origine"))) {
                        String subject = "Build Mail Error | " + ConfigParams.getAmbiente() + "/" + ConfigParams.getAzienda() + " - Errore nella costruzione della mail";
                        // sendMail(res, subject, ex);
                        writeDiagnoticaReport(res, ERRORE_COSTRUZIONE_MAIL, ex);
                    }
                } catch (SQLException e) {
                    log.info("Errore update messaggio_errore: ", e);
                }

                throw new SpedizioniereException("Errore nella costruzione dell'oggetto mail", ex);
            } finally {
                for (File tempFile : tempFiles) {
                    try {
                        tempFile.delete();
                    } catch (Throwable e) {
                        log.error("errore nella cancellazione del file: " + tempFile.getAbsolutePath(), e);
                    }
                }
            }
        }

//        private void sendMail(ResultSet res, String subject, Exception exception) {
//            log.info("Dentro send mail");
//            if (ConfigParams.getAzienda().equals("105") && ConfigParams.getAmbiente().toLowerCase().equals("test")) {
//                return;
//            }
//            try {
//                // Metadata
//                String[] to = new String[1];
//                log.info("setting to...");
//                if (ConfigParams.getAmbiente().toLowerCase().equals("test")) {  // Se in ambiente di test spedisci solo a Fayssel
////                    to = new String[2];
//                    to[0] = "f.sabir@nextsw.it";
////                    to[1] = "c.fiesoli@nsi.it";
//                } else {
//                    to[0] = ApplicationParams.getOtherPublicParam("EmailsAddressesAlert");
//                }
//                log.info("To setted!");
//                log.info("Getting from...");
//                String from = ApplicationParams.getOtherPublicParam("EmailsAlertFrom");
//                log.info("Getting host...");
//                String host = ApplicationParams.getOtherPublicParam("mailServerSmtpUrl");
//                log.info("Getting port...");
//                int port = Integer.valueOf(ApplicationParams.getOtherPublicParam("mailServerSmtpPort"));
//                // Email
//                //String subject = "SPEDIZIONIERE | " + ConfigParams.getAzienda() + "/" + ConfigParams.getAmbiente() + " - Tentativo di invio documento con lettera non firmata";
//                log.info("Composing message...");
//                String messageBody = "Dati del documento:<br/>";
//                messageBody += "id: " + res.getString("id") + "<br/>";
//                messageBody += "id_oggetto_origine: " + res.getString("id_oggetto_origine") + "<br/>";
//                messageBody += "tipo_oggetto_origine: " + res.getString("tipo_oggetto_origine") + "<br/>";
//                messageBody += "id_applicazione: " + res.getString("id_applicazione") + "<br/>";
//                messageBody += "Azienda: " + ConfigParams.getAzienda() + "<br/>";
//                messageBody += "Ambiente: " + ConfigParams.getAmbiente() + "<br/><br/><br/>";
//                messageBody += "Error log: <br/>";
//                messageBody += exception.toString();
//                log.info("Sending message...");
//                MailSender mailSender = new MailSender(to, from, host, subject, messageBody, port);
//                mailSender.send();
//            } catch (SQLException | MessagingException ex) {
//                log.info("Eccezione nell'invio dell'email d'errore agli sviluppatori: " + ex);
//            }
//        }
        private void writeDiagnoticaReport(ResultSet res, String tipologiaErrore, Exception exception) throws SQLException, NamingException {
            log.info("Dentro writeDiagnoticaReport");
//            if (ConfigParams.getAzienda().equals("105") && ConfigParams.getAmbiente().toLowerCase().equals("test")) {
//                log.info("Sono in test");
//                return;
//            }
            log.info("Controllo che l'errore non sia già in tabella");
            String querySelect = "SELECT 1 FROM diagnostica.report WHERE\n"
                    + "additional_data ->> 'tipologiaErrore' = ?\n"
                    + "and additional_data ->> 'idOggettoOrigine' = ?\n"
                    + "and risolto = false";
            try (Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(querySelect)) {
                ps.setString(1, tipologiaErrore);
                ps.setString(2, res.getString("id_oggetto_origine"));
                log.info("Query: " + ps);
                ResultSet executeQuery = ps.executeQuery();
                while (executeQuery.next()) {
                    log.info("Riga gia' presente");
                    return;
                }
            }

            log.info("Compongo l'additional data");
            JSONObject json = new JSONObject();
            json.put("id", res.getString("id"));
            json.put("idOggettoOrigine", res.getString("id_oggetto_origine"));
            json.put("tipoOggettoOrigine", res.getString("tipo_oggetto_origine"));
            json.put("idApplicazione", res.getString("id_applicazione"));
            json.put("azienda", ConfigParams.getAzienda());
            json.put("ambiente", ConfigParams.getAmbiente());
            json.put("exception", exception.toString());
            json.put("tipologiaErrore", tipologiaErrore);

            log.info("Preparo la query");
            String queryInsert = "INSERT INTO diagnostica.report\n"
                    + "(tipologia, additional_data, risolto, in_attesa_di_risoluzione)\n"
                    + "VALUES('ERRORE_SPEDIZIONIERE', ?::jsonb, false, false)";
            try (Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(queryInsert)) {
                ps.setString(1, json.toJSONString());
                log.info("Query: " + ps);
                ps.executeUpdate();
            }
        }

        private void controlloSpedizione() throws SpedizioniereException {
            log.info("ControlloSpedizione avviato!");
            String query = "SELECT "
                    + "id, id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine, descrizione_oggetto "
                    + "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                    + "WHERE id%? = ? "
                    + "AND (stato =?::bds_tools.stati_spedizione OR stato =?::bds_tools.stati_spedizione) "
                    + "AND da_ritentare =? FOR UPDATE";
            try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                ps.setString(3, StatiSpedizione.PRESA_IN_CARICO.toString());
                ps.setString(4, StatiSpedizione.ERRORE_SPEDIZIONE.toString());
                ps.setInt(5, -1);
                log.info("Query: " + ps);
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    try {
                        log.info("Richiamo controllaricevuta()");
                        controllaRicevuta(res, true);
                    } catch (Exception e) {
                        log.info(e);
                    }
                }
            } catch (SQLException | NamingException ex) {
                log.info(ex);
                throw new SpedizioniereException("Errore nel controllo spedizione", ex);
            }
        }

        private void controlloConsegna() throws SpedizioniereException {
            log.info("Started!");
            String query = "SELECT "
                    + "id, id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine, verifica_timestamp, descrizione_oggetto "
                    + "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                    + "WHERE id%? = ? AND (stato=?::bds_tools.stati_spedizione OR stato=?::bds_tools.stati_spedizione) AND da_ritentare = ? FOR UPDATE";
            try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                ps.setString(3, StatiSpedizione.SPEDITO.toString());
                ps.setString(4, StatiSpedizione.CONSEGNATO.toString());
                ps.setInt(5, -1);
                log.info("Query controllo consegna: " + ps);
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    try {
                        controllaRicevuta(res, false);
                    } catch (Exception e) {
                        log.info(e);
                    }
                }
                metodiSincronizzati.setDataFine();
            } catch (SQLException | NamingException ex) {
                log.info(ex);
                throw new SpedizioniereException("Errore nel reperimento delle mail dal DB per controlloConsegna()", ex);
            }
        }

        private void gestioneErrore() {
            log.info("Dentro gestioneErrore()");
            String query = "SELECT id, id_oggetto_origine, tipo_oggetto_origine, id_oggetto, stato, id_applicazione, utenti_da_notificare, messaggio_errore, descrizione_oggetto "
                    + "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                    + "WHERE id%? = ? AND ((stato = ?::bds_tools.stati_spedizione "
                    + "AND numero_errori >=?) "
                    + "OR stato = ?::bds_tools.stati_spedizione "
                    + "OR stato = ?::bds_tools.stati_spedizione "
                    + "OR stato = ?::bds_tools.stati_spedizione) "
                    + "AND notifica_inviata = ?";
            try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                ps.setString(3, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                ps.setInt(4, expired);
                ps.setString(5, StatiSpedizione.ERRORE_CONSEGNA.toString());
                ps.setString(6, StatiSpedizione.ERRORE_SPEDIZIONE.toString());
                ps.setString(7, StatiSpedizione.ERRORE_CONTRLLO_CONSEGNA.toString());
                ps.setInt(8, 0);
                log.info("Query: " + ps);
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    if (res.getString("utenti_da_notificare") != null) {
                        log.info("Utente da notificare presente");
                        gestisciErrore(res);
                    } else {
                        log.info("Utente da notificare non presente");
                    }
                }
            } catch (Exception ex) {
                log.info("Eccezione Select gestioneErrore()", ex);
            }
        }

        private void gestisciErrore(ResultSet res) throws Exception {
            log.info("Dentro gestisciErrore()");
            String nomeApp = null;
            String tokenApp = null;

            String query = "SELECT nome, token "
                    + "FROM " + ApplicationParams.getAuthenticationTable() + " "
                    + "WHERE  id_applicazione = ?";
            try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, res.getString("id_applicazione"));
                log.info("Query: " + ps);
                ResultSet r = ps.executeQuery();
                while (r.next()) {
                    nomeApp = r.getString("nome");
                    tokenApp = r.getString("token");
                }
            } catch (Exception e) {
                log.info("Eccezione nell'ottenimento del nome dell'applicazione: ", e);
            }
            String queryUrl = "SELECT url "
                    + "FROM " + ApplicationParams.getSpedizioniereApplicazioni() + " "
                    + "WHERE  id_applicazione_spedizioniere = ?";
            String url = null;
            try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(queryUrl)) {
                ps.setString(1, res.getString("id_applicazione"));
                log.info("QueryURL: " + ps);
                ResultSet r = ps.executeQuery();
                while (r.next()) {
                    url = r.getString("url");
                    log.info("URL: " + url);
                }
            } catch (Exception e) {
                log.info("Eccezione nell'ottenimento dell'url: ", e);
            }

            String urlCommand = url + ERRORE_SPEDIZIONE_MAIL + res.getString("id_oggetto_origine") + ";" + res.getString("tipo_oggetto_origine");
            UpdateBabelParams updateBabelParams = new UpdateBabelParams(res.getString("id_applicazione"), tokenApp, null, "false", "false", "insert");

            String[] utentiDaNotificare = null;
            utentiDaNotificare = res.getString("utenti_da_notificare").split(";");

            for (String utente : utentiDaNotificare) {
                updateBabelParams.addAttivita(
                        res.getString("id_oggetto_origine") + "_" + utente,
                        res.getString("id_oggetto_origine"),
                        utente,
                        "3",
                        "ErroreSpedizione",// + res.getString("stato"),
                        res.getString("descrizione_oggetto"),
                        null,
                        nomeApp,
                        null,
                        "Apri",
                        urlCommand,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        res.getString("id_oggetto_origine"),
                        res.getString("tipo_oggetto_origine"),
                        res.getString("id_oggetto_origine"),
                        res.getString("tipo_oggetto_origine"),
                        "group_" + res.getString("id_oggetto_origine"),
                        null,
                        null);
            }

            String retQueue = "notifica_errore" + "_" + ApplicationParams.getAppId() + "_updateBabelRetQueue_" + ApplicationParams.getServerId();
            log.info("Creazione Worker..");
            WorkerData wd = new WorkerData(ApplicationParams.getAppId(), "1", retQueue);
            log.info("Worker creato");
            wd.addNewJob("1", null, updateBabelParams);

//          RedisClient rd = new RedisClient("gdml", null);
//          rd.put(wd.getStringForRedis(), "chefingdml");
            RedisClient rd = new RedisClient(ApplicationParams.getRedisHost(), null);
            log.info("Add Worker to redis..");
            rd.put(wd.getStringForRedis(), ApplicationParams.getRedisInQueue());
            log.info("Worker added to redis");

            String extractedValue = rd.bpop(retQueue, 86400);
            WorkerResponse wr = new WorkerResponse(extractedValue);

            if (wr.getStatus().equalsIgnoreCase("ok")) {
                log.info("Worker status: Ok");
                String q = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                        + "SET notifica_inviata=? "
                        + "WHERE id = ?";
                try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(q)) {
                    ps.setInt(1, -1);
                    ps.setInt(2, res.getInt("id"));
                    log.info("Query: " + ps);
                    ps.executeUpdate();
                } catch (Exception e) {
                    log.info("Eccezione nella modifica del campo notifica_inviata: ", e);
                }
            } else {
                throw new ConvertPdfExeption("errore tornato dal masterchef: " + wr.getError());
//                System.out.println("KO");
            }
        }

        private void controllaRicevuta(ResultSet res, boolean controlloSpedizione) throws SpedizioniereException {
            log.info("Dentro controllaRicevuta");
            try {
                log.info("Extracting id mail to check...");
                long id = res.getLong("id");
                log.info("Id mail extracted: " + id);
                log.info("Creating spedizioniere client...");
                SpedizioniereClient spc = new SpedizioniereClient(getSpedizioniereUrl(), getUsername(), getPassword());
                log.info("Created!");
                log.info("Extracting id spedizione pecgw...");
                //String idMsg = String.valueOf(res.getInt("id_spedizione_pecgw"));
                String idMsg = res.getString("id_spedizione_pecgw");
                log.info("Extracted!");
                log.info("richiesta stato mail...");
                SpedizioniereStatus spStatus = spc.getStatus(idMsg);
                log.info("stato mail ricevuto: " + spStatus.getStatus().name());
                log.info("richiesta ricevute mail al Pecgw...");
                ArrayList<SpedizioniereRecepit> ricevuteMsg = spc.getRecepits(idMsg); // OTTENGO LE RICEVUTE DAL PECGW
                log.info("ottenute le ricevute dal Pecgw");
                int n = ricevuteMsg != null ? ricevuteMsg.size() : 0;
                log.info("numero di ricevute:" + n);

                if (spStatus.getStatus() == Status.ACCEPTED || spStatus.getStatus() == Status.CONFIRMED) {
                    log.info("stato ACCEPTED o CONFIRMED");

                    if (ricevuteMsg != null && ricevuteMsg.size() > 0) { // SE CI SONO RICEVUTE -- SE NON CI SONO CONTROLLO IL TIMESTAMP PER VEDERE SE è UNA MAIL O UNA PEC
                        log.info("Ricevute != null");

                        String ricevutaFromDb = "SELECT id "
                                + "FROM " + ApplicationParams.getRicevutePecTableName() + " "
                                + "WHERE uuid=?";
                        for (SpedizioniereRecepit spedizioniereRecepit : ricevuteMsg) { // PER OGNI RICEVUTA CONTROLLO SE è PRESENTE NEL DB
                            log.info("Tipo Ricevuta: " + spedizioniereRecepit.getTipo());

                            if (spedizioniereRecepit.getTipo() == TipoRicevuta.ERRORE_CONSEGNA
                                    || spedizioniereRecepit.getTipo() == TipoRicevuta.NON_ACCETTAZIONE
                                    || spedizioniereRecepit.getTipo() == TipoRicevuta.RILEVAZIONE_VIRUS
                                    || spedizioniereRecepit.getTipo() == TipoRicevuta.UNKNOWN) {

                                log.info("Ricevuta con errore");
                                String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                                        + "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ? "
                                        + "WHERE id = ?";
                                try (
                                        Connection conn = UtilityFunctions.getDBConnection();
                                        PreparedStatement ps = conn.prepareStatement(setStatoErroreInDb)) {
                                    ps.setString(1, StatiSpedizione.ERRORE_CONSEGNA.toString());
                                    ps.setInt(2, 0);
                                    ps.setLong(3, id);
                                    log.info("Query: " + ps);
                                    ps.executeUpdate();
                                } catch (Exception e) {
                                    log.info("eccezione nell'update su eccezione della funzione controllaRicevuta(): ", e);
                                }
                            } else {
                                log.info("Ricevuta senza errore");
                                try (
                                        Connection dbConnection = UtilityFunctions.getDBConnection();
                                        PreparedStatement ps = dbConnection.prepareStatement(ricevutaFromDb)) {
                                    ps.setString(1, spedizioniereRecepit.getUuid());
                                    log.info("Query: " + ps);
                                    ResultSet idRicevuta = ps.executeQuery(); // OTTENGO LA RICEVUTA DAL DB

                                    // QUERY CHE SCRIVE SUL DB LA RICEVUTA
                                    String insertRicevuta = "INSERT INTO " + ApplicationParams.getRicevutePecTableName()
                                            + "(tipo, uuid, id_oggetto_origine, tipo_oggetto_origine, data_inserimento, descrizione, id_spedizione_pec_globale) "
                                            + "VALUES (?::bds_tools.tipo_ricevuta, ?, ?, ?, now(), ?, ?)";

                                    // SE SUL DB NON è PRESENTE LA RICEVUTA LA SCRIVO
                                    if (!idRicevuta.next()) {
                                        log.info("idRicevuta != null");
                                        try (
                                                Connection settaUuid = UtilityFunctions.getDBConnection();
                                                PreparedStatement prepared = settaUuid.prepareStatement(insertRicevuta)) {
//                                            if(spStatus.getStatus() == Status.ACCEPTED){
//                                                prepared.setString(1, TipiRicevutaOLD.RICEVUTA_ACCETTAZIONE.toString());
//                                            }else{
//                                                prepared.setString(1, TipiRicevutaOLD.RICEVUTA_CONSEGNA.toString());
//                                            }
                                            prepared.setString(1, spedizioniereRecepit.getTipo().name());
                                            if (spedizioniereRecepit.getUuid() == null || spedizioniereRecepit.getUuid().equals("")) {
//                                                String[] to = new String[1];
//                                                to[0] = "f.sabir@nextsw.it";
//                                                String from = ApplicationParams.getOtherPublicParam("EmailsAlertFrom");
//                                                String host = ApplicationParams.getOtherPublicParam("mailServerSmtpUrl");
//                                                int port = Integer.valueOf(ApplicationParams.getOtherPublicParam("mailServerSmtpPort"));
//                                                // Email
//                                                String subject = "DISASTROOO! PANICOOOO!! RICEVUTA SENZA UUID!!! " + ConfigParams.getAzienda() + "/" + ConfigParams.getAmbiente();
//                                                String messageBody = "Dati della ricevuta:<br/>";
//                                                messageBody += "Tipo ricevuta: " + spedizioniereRecepit.getTipo().name() + "<br/>";
//                                                messageBody += "uuid: " + spedizioniereRecepit.getUuid() + "<br/>";
//                                                messageBody += "id_oggetto_origine: " + res.getString("id_oggetto_origine") + "<br/>";
//                                                messageBody += "tipo_oggetto_origine: " + res.getString("tipo_oggetto_origine") + "<br/>";
//                                                messageBody += "descrizione_oggetto: " + res.getString("descrizione_oggetto") + "<br/>";
//                                                messageBody += "Azienda: " + ConfigParams.getAzienda() + "<br/>";
//                                                messageBody += "Ambiente: " + ConfigParams.getAmbiente();
//                                                MailSender mailSender = new MailSender(to, from, host, subject, messageBody, port);
//                                                try {
//                                                    mailSender.send();
//                                                } catch (Exception e) {
//                                                    log.info("Errore nell'invio della mail a Fayssel Sabir!");
//                                                    throw new SpedizioniereException("Ricevuta senza uuid!");
//                                                }
                                                throw new SpedizioniereException("Errore la ricevuta non ha l'uuid");
                                            } else {
                                                prepared.setString(2, spedizioniereRecepit.getUuid());
                                            }
                                            prepared.setString(3, res.getString("id_oggetto_origine"));
                                            prepared.setString(4, res.getString("tipo_oggetto_origine"));
                                            prepared.setString(5, res.getString("descrizione_oggetto"));
                                            prepared.setLong(6, id);
                                            log.info("Query idRicevuta(INSERT): " + prepared);
                                            prepared.executeUpdate();
                                        } catch (NamingException ex) {
                                            log.info("Errore: ", ex);
                                            throw new SpedizioniereException("Errore nell'insert delle ricevute", ex);
                                        }

                                        // QUERY DI AGGIORNAMENTO DEGLI STATI DA CONFIRMED -> CONSEGNATO
                                        //                                    DA ACCEPTED -> PRESA IN CARICO
                                        String query = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                                                + "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ?, verifica_timestamp=now() "
                                                + "WHERE id = ?";
                                        try (
                                                Connection conn = UtilityFunctions.getDBConnection();
                                                PreparedStatement psquery = conn.prepareStatement(query)) {
                                            if (spStatus.getStatus() == Status.ACCEPTED) {
                                                psquery.setString(1, StatiSpedizione.SPEDITO.toString());
                                            } else {
                                                psquery.setString(1, StatiSpedizione.CONSEGNATO.toString());
                                            }
                                            if (spedizioniereRecepit.getTipo() == TipoRicevuta.AVVENUTA_CONSEGNA) {
                                                psquery.setInt(2, 0);
                                            } else {
                                                psquery.setInt(2, -1);
                                            }
                                            psquery.setLong(3, id);
                                            log.info("Query: " + psquery);
                                            psquery.executeUpdate();
                                        } catch (Exception ex) {
                                            log.info("eccezione aggiornamento stato SPEDITO o CONSEGNATO nella funzione controllaRicevuta()", ex);
                                        }
                                    }
                                } catch (NamingException | SpedizioniereException e) {
                                    log.info(e);
                                    throw new SpedizioniereException("Errore nel reperimento delle ricevute dal DB", e);
                                }
                            }
                        }

                    }

                    // NEL CASO SIA UNA MAIL NORMALE E NON UNA PEC DOPO TOT GIORNI DEVE ESSERE MESSA A CONFIRMED E SETTARE IL TIMESTAMP
                    log.info("SONO NELLO STATO CONTROLLO CONSEGNA OPPURE NON HO RICEVUTE");
                    // calcolo i giorni, DIFF TRA IL TIMESTAMP DI E QUELLO SU DB
                    Long ggDiff = getDays(res.getTimestamp("verifica_timestamp"), new Timestamp(new Date().getTime()));
                    log.info("Giorni differenza: " + ggDiff);

                    if (ggDiff >= getExpired()) {
                        // QUERY DI AGGIORNAMENTO DEGLI STATI SU DB da CONFIRMED -> INVIATO
                        String query = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                                + "SET stato=?::bds_tools.stati_spedizione, da_ritentare = ?, verifica_timestamp=now() "
                                + "WHERE id = ?";
                        try (
                                Connection conn = UtilityFunctions.getDBConnection();
                                PreparedStatement ps = conn.prepareStatement(query)) {
                            //ps.setString(1, StatiSpedizione.CONSEGNATO.toString());
                            ps.setString(1, StatiSpedizione.INVIATO.toString());
                            ps.setInt(2, 0);
                            ps.setLong(3, id);
                            log.info("Query: " + ps);
                            ps.executeUpdate();
                        } catch (NamingException ex) {
                            log.info("Eccezione aggiornamento stato email expired: ", ex);
                        }
                    } else {
                        log.info("attendo ancora la ricevuta di consegna...");
                    }
//                        Prima il controllo veniva fatto solo per le spedizioni in stato accettato
//                        if (spStatus.getStatus() == Status.ACCEPTED) {
//
//                        }

                } else if (spStatus.getStatus() == SpedizioniereStatus.Status.ERROR) { // NEL CASO DI STATO DI ERRORE
                    log.info("Dentro ERROR");
                    boolean aggiornaSpedizione = true;
                    if (ricevuteMsg != null && ricevuteMsg.size() > 0) {
                        String ricevutaFromDb = "SELECT id "
                                + "FROM " + ApplicationParams.getRicevutePecTableName() + " "
                                + "WHERE uuid=?";

                        String ricevutePreavvisoQuery = "SELECT id, data_inserimento, uuid "
                                + "FROM " + ApplicationParams.getRicevutePecTableName() + " "
                                + "WHERE id_spedizione_pec_globale=? AND tipo=?::bds_tools.tipo_ricevuta";

                        String insertRicevuta = "INSERT INTO " + ApplicationParams.getRicevutePecTableName()
                                + "(tipo, uuid, id_oggetto_origine, tipo_oggetto_origine, data_inserimento, descrizione, id_spedizione_pec_globale) "
                                + "VALUES (?::bds_tools.tipo_ricevuta, ?, ?, ?, now(), ?, ?)";

                        for (SpedizioniereRecepit spedizioniereRecepit : ricevuteMsg) { // PER OGNI RICEVUTA CONTROLLO SE è PRESENTE NEL DB
                            log.info("Dentro FOR dell' ERROR");
                            if (spedizioniereRecepit.getTipo() == TipoRicevuta.PREAVVISO_ERRORE_CONSEGNA) {
                                try (
                                        Connection conn = UtilityFunctions.getDBConnection();
                                        PreparedStatement ps = conn.prepareStatement(ricevutePreavvisoQuery, ResultSet.TYPE_SCROLL_INSENSITIVE)) {
                                    ps.setInt(1, res.getInt("id"));
                                    ps.setString(2, TipoRicevuta.PREAVVISO_ERRORE_CONSEGNA.toString());
                                    log.info("Query preavvisi: " + ps);
                                    ResultSet resPreavvisi = ps.executeQuery();
                                    if (resPreavvisi.next()) {
                                        resPreavvisi.beforeFirst();
                                        while (resPreavvisi.next()) {
                                            if (!resPreavvisi.getString("uuid").equals(spedizioniereRecepit.getUuid())) {
                                                try (
                                                        Connection settaUuid = UtilityFunctions.getDBConnection();
                                                        PreparedStatement prepared = settaUuid.prepareStatement(insertRicevuta)) {
                                                    prepared.setString(1, spedizioniereRecepit.getTipo().name());
                                                    prepared.setString(2, spedizioniereRecepit.getUuid());
                                                    prepared.setString(3, res.getString("id_oggetto_origine"));
                                                    prepared.setString(4, res.getString("tipo_oggetto_origine"));
                                                    prepared.setString(5, res.getString("descrizione_oggetto"));
                                                    prepared.setLong(6, id);

                                                    log.info("esecuzione query di inserimento della ricevuta: " + prepared.toString());
                                                    prepared.executeUpdate();
                                                } catch (NamingException ex) {
                                                    log.info("Errore: ", ex.getMessage());
                                                    throw new SpedizioniereException("Errore nell'insert delle ricevute", ex);
                                                }
                                            } else {
                                                Timestamp dataInserimento = resPreavvisi.getTimestamp("data_inserimento");
                                                Timestamp ora = new Timestamp(new Date().getTime());
                                                Long giorniTrascorsi = getDays(ora, dataInserimento);
                                                if (giorniTrascorsi < expired) {
                                                    aggiornaSpedizione = false;
                                                }
                                            }
                                        }
                                    } else {
                                        aggiornaSpedizione = false;
                                        try (
                                                Connection settaUuid = UtilityFunctions.getDBConnection();
                                                PreparedStatement prepared = settaUuid.prepareStatement(insertRicevuta)) {
                                            prepared.setString(1, spedizioniereRecepit.getTipo().name());
                                            prepared.setString(2, spedizioniereRecepit.getUuid());
                                            prepared.setString(3, res.getString("id_oggetto_origine"));
                                            prepared.setString(4, res.getString("tipo_oggetto_origine"));
                                            prepared.setString(5, res.getString("descrizione_oggetto"));
                                            prepared.setLong(6, id);

                                            log.info("esecuzione query di inserimento della ricevuta: " + prepared.toString());
                                            prepared.executeUpdate();
                                        } catch (NamingException ex) {
                                            log.info("Errore: ", ex.getMessage());
                                            throw new SpedizioniereException("Errore nell'insert delle ricevute", ex);
                                        }
                                    }

                                } catch (NamingException ex) {
                                    log.info("Eccezione aggiornamento stato email expired: ", ex);
                                }
                            }
                            if (spedizioniereRecepit.getUuid() == null || spedizioniereRecepit.getUuid().equals("")) {
                                throw new SpedizioniereException("Errore la ricevuta non ha l'uuid");
                            }
                            try (
                                    Connection dbConnection = UtilityFunctions.getDBConnection();
                                    PreparedStatement ps = dbConnection.prepareStatement(ricevutaFromDb)) {
                                ps.setString(1, spedizioniereRecepit.getUuid());
                                log.info("eseguo la query per verificare se la ricevuta esiste già: " + ps.toString());
                                ResultSet ricevutaResultSet = ps.executeQuery(); // OTTENGO LA RICEVUTA DAL DB

                                // QUERY CHE SCRIVE SUL DB LA RICEVUTA
                                // SE SUL DB NON è PRESENTE LA RICEVUTA LA SCRIVO
                                if (ricevutaResultSet == null || !ricevutaResultSet.next()) {
                                    log.info("Dentro idRicevuta == null");
                                    try (
                                            Connection settaUuid = UtilityFunctions.getDBConnection();
                                            PreparedStatement prepared = settaUuid.prepareStatement(insertRicevuta)) {
                                        //                                    if(spStatus.getStatus() == Status.ACCEPTED){
                                        //                                        log.info(" IF Status accepted");
                                        //                                        prepared.setString(1, TipiRicevutaOLD.RICEVUTA_ACCETTAZIONE.toString());
                                        //                                    }else{
                                        //                                        log.info("else");
                                        //                                        prepared.setString(1, TipiRicevutaOLD.RICEVUTA_CONSEGNA.toString());
                                        //                                    }
                                        prepared.setString(1, spedizioniereRecepit.getTipo().name());
                                        prepared.setString(2, spedizioniereRecepit.getUuid());
                                        prepared.setString(3, res.getString("id_oggetto_origine"));
                                        prepared.setString(4, res.getString("tipo_oggetto_origine"));
                                        prepared.setString(5, res.getString("descrizione_oggetto"));
                                        prepared.setLong(6, id);

                                        log.info("esecuzione query di inserimento della ricevuta: " + prepared.toString());
                                        prepared.executeUpdate();
                                    } catch (NamingException ex) {
                                        log.info("Errore: ", ex.getMessage());
                                        throw new SpedizioniereException("Errore nell'insert delle ricevute", ex);
                                    }
                                }
                            } catch (NamingException e) {
                                log.info(e);
                                throw new SpedizioniereException("Errore nel reperimento delle ricevute", e);
                            }
                        }
                    }
                    if (aggiornaSpedizione) {
                        log.info("Verra eseguito update");
                        String query = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                                + "SET stato=?::bds_tools.stati_spedizione, verifica_timestamp= now() "
                                + "WHERE id = ?";
                        try (
                                Connection conn = UtilityFunctions.getDBConnection();
                                PreparedStatement ps = conn.prepareStatement(query)) {
                            ps.setString(1, StatiSpedizione.ERRORE_CONSEGNA.toString());
                            ps.setLong(2, id);
                            log.info("Query: " + ps);
                            ps.executeUpdate();
                        } catch (NamingException ex) {
                            log.info("eccezione aggiornamento stato ERRORE nella funzione controllaRicevuta()", ex);
                        }
                    }
                }
            } catch (Exception ex) {
                try {
                    setMessaggioErrore("Errore nel reperire le ricevute", res.getLong("id"));
                } catch (SQLException e) {
                    log.info("Errore update messaggio_errore: ", e);
                }
                log.info(ex);
                throw new SpedizioniereException("Errore nel reperimento dello stato o delle ricevute dal Pecgw", ex);
            }
        }

        private void stepOn() throws SQLException, NamingException {
            log.info("Dentro StepOn");

            String query = "SELECT id_oggetto_origine "
                    + "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                    + "WHERE  stato_avanzamento_processo = ?::bds_tools.type_avanzamento_processo "
                    + "GROUP BY id_oggetto_origine";
            try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, StatiAvanzamentoProcesso.DA_AVANZARE.toString());
                log.info("Query: " + ps);
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    verificaStepOn(res.getString("id_oggetto_origine"));
                }
            }
        }

        private void verificaStepOn(String idOggettoOrigine) throws SQLException, NamingException {
            log.info("Dentro VerificaStepOn");

            String queryVerifica = "SELECT DISTINCT(spg.process_id) "
                    + "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " spg "
                    + "WHERE id_oggetto_origine = ? "
                    + "AND stato_avanzamento_processo != ?::bds_tools.type_avanzamento_processo "
                    + "AND not exists(SELECT 1 "
                    + "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                    + "WHERE id_oggetto_origine = spg.id_oggetto_origine "
                    + "AND stato not in (?::bds_tools.stati_spedizione, ?::bds_tools.stati_spedizione, ?::bds_tools.stati_spedizione, ?::bds_tools.stati_spedizione))";

            try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(queryVerifica)) {
                ps.setString(1, idOggettoOrigine);
                ps.setString(2, StatiAvanzamentoProcesso.AVANZATO.toString());
                ps.setString(3, StatiSpedizione.SPEDITO.toString());
                ps.setString(4, StatiSpedizione.CONSEGNATO.toString());
                ps.setString(5, StatiSpedizione.ANNULLATO.toString());
                ps.setString(6, StatiSpedizione.ERRORE_CONSEGNA.toString());
                log.info("Query: " + ps);
                ResultSet res = ps.executeQuery();
                if (res.next()) {
                    log.info("Stepon consentito per l'oggetto: " + idOggettoOrigine);
                    String processId = res.getString(1);
                    doStepOn(processId);
                    //estraiMail(idOggettoOrigine);
                } else {
                    log.info("Stepon non possibile o gia' effettuato per l'oggetto: " + idOggettoOrigine);
                }
            }
        }

//        private void estraiMail(String idOggettoOrigine){
//            log.info("Dentro EstraMail");
//            String query =  "SELECT process_id " +
//                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
//                            "WHERE id_oggetto_origine = ?";
//            try (
//                Connection conn = UtilityFunctions.getDBConnection();
//                PreparedStatement ps = conn.prepareStatement(query)
//            ) {
//                ps.setString(1, idOggettoOrigine);
//                log.info("Query: " + ps);
//                ResultSet res = ps.executeQuery();
//                while (res.next()) {
//                    doStepOn(res.getString("process_id"));
//                }
//            } catch (SQLException ex) {
//                log.info("Errore caricamento mail relative a un documento: ", ex);
//            } catch (NamingException ex) {
//                log.info("Errore caricamento mail relative a un documento: ", ex);
//            }
//        }
        private void doStepOn(String processId) {
            log.info("Dentro do Step");
            try {
                BdmClientInterface bdmClient = new RemoteBdmClientImplementation(ApplicationParams.getBdmRestBaseUri());
                Step currentStep = bdmClient.getCurrentStep(processId);
//                String currentStep = bdmClient.getCurrentStep(processId).getStepType();
                log.info("Process status: " + bdmClient.getProcessStatus(processId));
                log.info("current step: " + currentStep);

                if (bdmClient.getProcessStatus(processId) == Bdm.BdmStatus.FINISHED || currentStep.getStepType().equals(ProtocolloInUscitaProcess.Steps.ASPETTA_SPEDIZIONI.name())) {
                    log.info("eseguo lo stepon sul processo: " + processId);
                    bdmClient.stepOn(processId, new Bag());
                    String query = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                            + "SET stato_avanzamento_processo = ?::bds_tools.type_avanzamento_processo "
                            + "WHERE process_id = ?";
                    try (
                            Connection conn = UtilityFunctions.getDBConnection();
                            PreparedStatement ps = conn.prepareStatement(query)) {
                        ps.setString(1, StatiAvanzamentoProcesso.AVANZATO.toString());
                        ps.setString(2, processId);
                        log.info("Query: " + ps);
                        ps.executeUpdate();
                    } catch (Exception ex) {
                        log.error("Errore nel settare lo stato sul db ad avanzato: ", ex);
                    }
                }
            } catch (Exception ex) {
                log.error("Errore nell'instanziare il bdmclient: ", ex);
            }
        }
    }

    private void setMessaggioErrore(String messaggio, Long id) {
        String queryMessaggioErrore = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " "
                + "SET messaggio_errore = ? "
                + "WHERE id = ?";
        try (
                Connection connMessaggioErrore = UtilityFunctions.getDBConnection();
                PreparedStatement psMessaggioErrore = connMessaggioErrore.prepareStatement(queryMessaggioErrore)) {
            psMessaggioErrore.setString(1, messaggio);
            psMessaggioErrore.setLong(2, id);
            psMessaggioErrore.executeUpdate();

        } catch (SQLException | NamingException e) {
            log.info("Errore nell'update del messaggio di errore: ", e);
        }
    }

    private Boolean canSend(ResultSet res) {
        log.info("canSend started");
        String query = "SELECT attivo "
                + "FROM " + ApplicationParams.getSpedizioniereApplicazioni() + " "
                + "WHERE  id_applicazione_spedizioniere = ? ";
        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query);) {
            String idApplicazione = res.getString("id_applicazione");
            ps.setString(1, idApplicazione);

            log.info("Query canSend: " + ps);
            ResultSet r = ps.executeQuery();
            if (r.next()) {
                log.info("attivo == " + r.getInt("attivo"));
                return r.getInt("attivo") == 0 ? false : true;
            }
        } catch (SQLException | NamingException ex) {
            log.info("ECCEZIONE: ", ex);
            return false;
        }
        return false;
    }

    private Long getDays(Timestamp old, Timestamp now) {
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

    private java.sql.Timestamp getTimestamp(String data) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date parsedDate = dateFormat.parse(data);
        return (new java.sql.Timestamp(parsedDate.getTime()));
    }

    private boolean canStartProcess() {
        log.info("check if Spedizioniere process can start...");
        String query = "SELECT val_parametro "
                + "FROM " + ApplicationParams.getParametriPubbliciTableName() + " "
                + "WHERE  nome_parametro = ? ";
        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query);) {
            ps.setString(1, SPEDIZIONIERE_ACTIVE);
            ResultSet r = ps.executeQuery();
            if (r.next()) {
                log.info("spedizioniere_attivo == " + r.getInt("val_parametro"));
                return r.getInt("val_parametro") != 0;
            }
        } catch (SQLException | NamingException ex) {
            log.info("ECCEZIONE: ", ex);
            return false;
        }
        return false;
    }

    public class MetodiSincronizzati {

        private boolean canStartControllo;
        private boolean haveTocheck;
        //private  final Logger log = LogManager.getLogger(Spedizioniere.class);
        private int numThread;

        public MetodiSincronizzati() {
            this.canStartControllo = false;
            this.haveTocheck = true;
            this.numThread = 0;
        }

        protected synchronized boolean check() throws SQLException, NamingException, SpedizioniereException {
            log.info("Dentro Check()");
            // Conto quanti thread ho in totale cosi da poter far settare la data di fine all'ultimo con il setDataFine()
            numThread++;
            if (canStartControllo) { // Il primo thread troverà sempre false questa condizione e sarà costretto a fare il check
                log.info("Check: First if: return true");
                return true;
            } else if (haveTocheck) { // Il primo thread fa il check poi setto la variabile a false così i successivi thread non lo effettuano
                log.info("Elese if havetocheck");
                haveTocheck = false;
                String queryRange = "SELECT data_inizio "
                        + "FROM " + ApplicationParams.getServiziTableName() + " "
                        + "WHERE nome_servizio=?";

                Timestamp dataInizio = null;

                try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(queryRange);) {
                    ps.setString(1, "spedizioniere_controllo_consegna");

                    ResultSet res = ps.executeQuery();
                    while (res.next()) {
                        dataInizio = res.getTimestamp("data_inizio");
                    }
                } catch (Exception ex) {
                    log.info("eccezione nella select param servizio: ", ex);
                }

                if (dataInizio != null) {
                    log.info("dataInizio != null");
                    log.info("dataInizio = " + dataInizio);
                    Timestamp ora = new Timestamp(new Date().getTime());
                    log.info("Ora: " + ora);
//                    Long giorni =  getDays(dataInizio, ora);
                    Duration duration = new Duration(new DateTime(dataInizio.getTime()), new DateTime(ora.getTime()));
                    Long differenzaOre = duration.getStandardHours();
//                    Long differenzaMinuti = duration.getStandardMinutes();
//                    log.info("Controllo consegna giorni differenza: " + giorni);
                    log.info("Controllo consegna ore differenza: " + differenzaOre);
//                    log.info("Controllo consegna minuti differenza: " + differenzaMinuti);
                    if (differenzaOre != null) {

                        if (differenzaOre >= 2) { // Se il controlloSpedizione() è stato effettuato almeno due ore fa
                            log.info("Controllo consegna will start soon...");
                            log.info("Setting data inizio...");
                            setDataInizio();
                            log.info("done!");
                            canStartControllo = true; // Setto la variabile a true così i successivi thread partono senza effettuare il check
                            log.info("Returning true...");
                            return true; // Ritorno true per far eseguire controlloSpedizione() al primo thread
                        } else {
                            canStartControllo = false;
                            return false;
                        }
                    }
                    throw new SpedizioniereException("Errore nel calcolo nella differenza giorni"); // Sollevo Eccezione perchè la dataInizio è presente
                    // ma si è verificato un errore nel calcolo dei giorni
                } else {
                    log.info("dataInizio == null");
                    log.info("The service will start for the first time...");
                    // Se la dataInizio è uguale a null la setto e ritorno true per far partire controlloSpedizione() al primo thread
                    // e setto canControllo per per i successivi
                    log.info("Setting data inizio...");
                    setDataInizio();
                    log.info("done!");
                    canStartControllo = true;
                    log.info("Returning true...");
                    return true;
                }
            }
            return false;
        }

        private void setDataInizio() throws SQLException, NamingException {
            log.info("Dentro setDataInizio()");
            String query = "UPDATE " + ApplicationParams.getServiziTableName() + " "
                    + "SET data_inizio=now() "
                    + "WHERE nome_servizio ='spedizioniere_controllo_consegna'";
            try (
                    Connection conn = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = conn.prepareStatement(query)) {
                log.info("Query: " + ps);
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
                log.info("Eccezione nel calcolo della data: " + ex);
            }

            return diffDays;
        }
         */
        protected synchronized void setDataFine() throws SQLException, NamingException {
            log.info("Dentro setDataFine()");
            numThread--;
            if (numThread == 0) {
                log.info("Dentro setDataFine() numThread = 0");
                String query = "UPDATE " + ApplicationParams.getServiziTableName() + " "
                        + "SET data_fine=now() "
                        + "WHERE nome_servizio ='spedizioniere_controllo_consegna'";
                try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(query)) {
                    log.info("Query: " + ps);
                    ps.executeUpdate();
                }
            }
        }
    }

}
