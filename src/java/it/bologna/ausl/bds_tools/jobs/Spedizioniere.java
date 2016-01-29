package it.bologna.ausl.bds_tools.jobs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import it.bologna.ausl.bds_tools.exceptions.SpedizioniereException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.spedizioniereclient.SpedizioniereAttachment;
import it.bologna.ausl.spedizioniereclient.SpedizioniereClient;
import it.bologna.ausl.spedizioniereclient.SpedizioniereMessage;
import it.bologna.ausl.spedizioniereclient.SpedizioniereRecepit;
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
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.naming.NamingException;
import org.apache.commons.io.IOUtils;
import org.apache.jasper.tagplugins.jstl.core.Catch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    
    private String connectUri, driver;
    private static final Logger log = LogManager.getLogger(Spedizioniere.class);
    private final ExecutorService pool;
    private Integer maxThread;
    private int timeOutHours;

    public int getTimeOutHours() {
        return timeOutHours;
    }

    public void setTimeOutHours(int timeOutHours) {
        this.timeOutHours = timeOutHours;
    }
    
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
    
    public Spedizioniere() throws SpedizioniereException {
        
        try {
            Connection dbConnection = UtilityFunctions.getDBConnection();
            maxThread = Integer.valueOf(ApplicationParams.getMaxThreadSpedizioniere());
            pool = Executors.newFixedThreadPool(maxThread);
        }
        catch(Exception ex) {
            throw new SpedizioniereException("Errore nella costruzione dell'oggetto spedizioniere", ex);
        }
        
        
        
    }
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
    
        log.info("Job Spedizioniere started");
        try {
            
            for(int i = 1; i <= maxThread; i++){
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

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
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
                spedizione();
            }
            catch (Exception ex) {
                log.error(ex);
            }
            controlloSpedizione();
            controlloConsegna();
            gestioneErrore();
        }
        
        
        private void spedizione() throws SpedizioniereException {
            String query =  "SELECT " +
                            "id, id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine, id_oggetto, " +
                            "stato, id_applicazione, verifica_timestamp, oggetto_da_spedire_json, " +
                            "utenti_da_notificare, notifica_inviata, numero_errori, da_ritentare " +
                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "WHERE id%? = ? AND (stato = ? or stato = ?) AND da_ritentare = ? for update";

            try (
                    Connection dbConnection = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = dbConnection.prepareStatement(query)
                ) {

                ps.setInt(1, threadsTotal);
                ps.setInt(2, threadSerial);
                ps.setString(3, StatiSpedizione.DA_INVIARE.toString());
                ps.setString(4, StatiSpedizione.ERRORE_PRESA_INCARICO.toString());
                ps.setBoolean(5, true);

                ResultSet res = ps.executeQuery();
                
                while (res.next()) {
                    try {
                        spedisci(res);
                    }
                    catch (SpedizioniereException ex) {
                        log.error(ex);
                    }
                }
            }
            catch(Exception ex) {
                throw new SpedizioniereException("Errore nel reperimento dei documenti da spedire", ex);
            }
        }
        
        private void spedisci(ResultSet res) throws SpedizioniereException {
            
            SpedizioniereClient spc = new SpedizioniereClient("https://gdml.internal.ausl.bologna.it/spedizioniere", "testapp", "testapp");
            ArrayList<SpedizioniereAttachment> attachments = new ArrayList<SpedizioniereAttachment>();
            String mongoUri = ApplicationParams.getMongoRepositoryUri();
            MongoWrapper mongo;
            
            try (
                    Connection dbConnection = UtilityFunctions.getDBConnection();
//                    PreparedStatement ps = dbConnection.prepareStatement(query)
                ) {
                String externalId = res.getString("id_oggetto");
                String oggettoDaSpedire = res.getString("oggetto_da_spedire_json");
                
                String idOggetto = res.getString("id_oggetto_origine");
                String tipoOggetto =  res.getString("tipo_oggetto_origine");
                
                int numErrori = res.getInt("numero_errori");
                
                Mail mail = Mail.parse(oggettoDaSpedire);
                SpedizioniereMessage message = new SpedizioniereMessage(mail.getFrom(), mail.getTo(), mail.getCc(), mail.getSubject(), mail.getMessage(), externalId);
                mongo = new MongoWrapper(mongoUri);
                
                /*mail.getAttachments().stream().map((attachment) -> {
                    // scaricare da mongo l'inputstream
                    
                    InputStream is = mongo.get(attachment.getUuid());                    
                    SpedizioniereAttachment att = new SpedizioniereAttachment(attachment.getName(), attachment.getMimetype(), is);
                    return att;
                }).forEach((att) -> {
                    attachments.add(att);
                });
                message.setAttachments(attachments);*/
                // versione classica
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
                    //SpedizioniereAttachment att = new SpedizioniereAttachment(attachment.getName(), attachment.getMimetype(), is);
                    //attachments.add(att);
                   SpedizioniereAttachment att = new SpedizioniereAttachment(attachment.getName(), attachment.getMimetype(), tmpFile);
                   attachments.add(att);
                }
                message.setAttachments(attachments);
                
                // Utilizziamo un altro Try Catch per evitare di fare il rollback su tutta la funzione
                try{
                    String messageId = spc.sendMail(message, false);
                    String updateMessageId =    "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET id_spedizione_pecgw=? and stato=? " +
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
                }catch(Exception ex){
                    String setStatoErroreInDb = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET stato=? AND da_ritentare = ? and numero_errori=? " +
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

                        ps.executeUpdate();
                    }
                }finally{
                    
                }            
            }
            catch(Exception ex) {
                throw new SpedizioniereException("Errore nel reperimento dei documenti da spedire", ex);
            }
        }
        private void controlloSpedizione(){
    /*        String query = "SELECT " +
                            "id_spedizione_pecgw, id_oggetto_origine, tipo_oggetto_origine " +
                            "FROM " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                            "WHERE stato = '" + StatiSpedizione.PRESA_IN_CARICO + "' OR da_ritentare = '" + true + "'";
            
            try (
                Connection conn = UtilityFunctions.getDBConnection();
                PreparedStatement ps = conn.prepareStatement(query)
            ){
                ResultSet res = ps.executeQuery();
                while(res.next()){
                    try {
                        controllaRicevuta(res);
                    } catch (Exception e) {
                    }
                }
                int idMessage = res.getInt("id_spedizione_pecgw");
            } catch (SQLException ex) {
                java.util.logging.Logger.getLogger(Spedizioniere.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NamingException ex) {
                java.util.logging.Logger.getLogger(Spedizioniere.class.getName()).log(Level.SEVERE, null, ex);
            }
    */    }
        
        private void controllaRicevuta(ResultSet res){
    /*        SpedizioniereClient spc = new SpedizioniereClient("https://gdml.internal.ausl.bologna.it/spedizioniere", "testapp", "testapp");
            
            try {
                String idMsg = String.valueOf(res.getInt("id_spedizione_pecgw"));
                SpedizioniereStatus spStatus = spc.getStatus(idMsg);
                if (spStatus.equals("ACCEPTED") || spStatus.equals("CONFIRMED")) {
                    ArrayList<SpedizioniereRecepit> ricevuteMsg = spc.getRecepits(idMsg);
                    
                    // ANNESSI E NOME TABELLA ANNESSI
                    
                    String query = "UPDATE " + ApplicationParams.getSpedizioniPecGlobaleTableName() + " " +
                                                "SET stato=? AND verifica_timestamp=? " +
                                                "WHERE id_oggetto_origine=? and tipo_oggetto_origine=?";
                    
                    try (
                        Connection conn = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = conn.prepareStatement(query)
                    ) {
                        if (spStatus.equals("ACCEPTED")) {
                            ps.setString(1, StatiSpedizione.PRESA_IN_CARICO.toString());
                        }else{
                            ps.setString(1, StatiSpedizione.CONSEGNATO.toString());
                        }
                        ps.setTimestamp(2, new Timestamp(new Date().getTime()));
                        ps.setString(3, res.getString("id_oggetto_origine"));
                        ps.setString(4, res.getString("tipo_oggetto_origine"));

                        ps.executeUpdate();
                    } catch (NamingException ex) {
                        java.util.logging.Logger.getLogger(Spedizioniere.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    
                    
                }
            } catch (SQLException ex) {
                java.util.logging.Logger.getLogger(Spedizioniere.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Spedizioniere.class.getName()).log(Level.SEVERE, null, ex);
            }
            
    */    }
        
        private void controlloConsegna(){}
    
        private void gestioneErrore(){}
    
    }
    
}
