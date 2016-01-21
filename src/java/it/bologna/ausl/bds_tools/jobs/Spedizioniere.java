package it.bologna.ausl.bds_tools.jobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */
public class Spedizioniere implements Job{
    
    private String connectUri, driver;
    private static final Logger log = LogManager.getLogger(Spedizioniere.class);
    
    public Spedizioniere() {}
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
    
        log.info("Job Spedizioniere started");
        try {
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
    
    
    private boolean spedizione(){return true;}
    
    private boolean controlloSpedizione(){return true;}
    
    private boolean controlloConsegna(){return true;}
    
    private boolean gestioneErrore(){return true;}
    
    
    
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
    
}
