/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.jobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */
public class AzzeratoreUidEmailScaricate implements Job {

    private String connectUri, driver;
    private static final Logger log = Logger.getLogger(AzzeratoreUidEmailScaricate.class);

    public AzzeratoreUidEmailScaricate() {
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("Job AzzeratoreUidEmailScaricate started");
        try {
            Class.forName(driver);
            Connection c = DriverManager.getConnection(connectUri);
            c.setAutoCommit(true);
            Statement s = c.createStatement();
            s.executeUpdate("update pecgw.mail_config set lastuid=0");
            c.close();
        } catch (Throwable t) {
            log.fatal("AzzeratoreUidEmailScaricate: Errore in update db per download mail", t);
        } finally {

            log.info("Job AzzeratoreUidEmailScaricate finished");
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

}
