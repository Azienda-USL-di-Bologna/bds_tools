package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.balboclient.BalboClient;
import it.bologna.ausl.balboclient.classes.AllegatoPubblicazione;
import it.bologna.ausl.balboclient.classes.Pubblicazione;
import it.bologna.ausl.balboclient.classes.PubblicazioneAlbo;
import it.bologna.ausl.bds_tools.exceptions.VersatoreParerException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.Registro;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.PubblicazioneIoda;
import it.bologna.ausl.ioda.iodaobjectlibrary.SottoDocumento;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;
import javax.naming.NamingException;
import javax.servlet.ServletException;
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
public class VersatoreParer implements Job {

    private static final Logger log = LogManager.getLogger(VersatoreParer.class);
 
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.debug("Versatore ParER Started");

        try {
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
          Thread.sleep(5000);
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(VersatoreParer.class.getName()).log(Level.SEVERE, null, ex);
        }

            log.debug("Versatore ParER Ended");
       // }
    }
    
    private void setParameters(String serviceName) throws VersatoreParerException{
        String query = "SELECT id_servizio, nome_servizio, data_inizio, data_fine, id_applicazione, " +
                       "attivo, dati " +
                       "FROM bds_tools.servizi";
         
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(query)
        ){
             
        }
        catch(Exception ex) {
            throw new VersatoreParerException("Errore nel reperimento dei dati di servizio", ex);
        }
    }
    
}
