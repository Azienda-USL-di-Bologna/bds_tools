package it.bologna.ausl.bds_tools.jobs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import it.bologna.ausl.bds_tools.exceptions.SpedizioniereException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.naming.NamingException;
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
    
    public Spedizioniere() {}
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
    
        log.info("Job Spedizioniere started");
        try {
            
            Connection dbConnection = UtilityFunctions.getDBConnection();
            
            Class.forName(driver);
            Connection c = DriverManager.getConnection(connectUri);
            c.setAutoCommit(true);
            Statement s = c.createStatement();
            s.executeUpdate("");
            c.close();
        } catch (Throwable t) {
            log.fatal("Spedizioniere: Errore ...", t);
        } finally {

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
            
            
            
            try (
                    Connection dbConnection = UtilityFunctions.getDBConnection();
//                    PreparedStatement ps = dbConnection.prepareStatement(query)
                ) {
                
            }
            catch(Exception ex) {
                throw new SpedizioniereException("Errore nel reperimento dei documenti da spedire", ex);
            }
        }
        private void controlloSpedizione(){}
    
        private void controlloConsegna(){}
    
        private void gestioneErrore(){}
    
    }
    
}
