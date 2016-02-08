/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.exceptions.SpedizioniereException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import javax.naming.NamingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Fayssel
 */
public class MetodiSincronizzati {
    
    private  boolean canStartControllo;
    private  boolean haveTocheck;
    private  final Logger log = LogManager.getLogger(Spedizioniere.class);
    private  int numThread;
    
    public MetodiSincronizzati(){
        this.canStartControllo = false;
        this.haveTocheck = true;
        this.numThread = 0;
    }
    
    protected synchronized boolean check() throws SQLException, NamingException, SpedizioniereException{
        // Conto quanti thread ho in totale cosi da poter far settare la data di fine all'ultimo con il setDataFine()
        numThread++;
        if (canStartControllo) { // Il primo thread troverà sempre false questa condizione e sarà costretto a fare il check
            return true;
        }else if(haveTocheck){ // Il primo thread fa il check poi setto la variabile a false così i successivi thread non lo effettuano
            haveTocheck = false;
            String queryRange = "SELECT nome_parametro, val_parametro " +
                                "FROM " + ApplicationParams.getParametriPubbliciTableName() + " " +
                                "WHERE nome_parametro=?";
            String nomeServizio = null;
            Timestamp dataInizio = null;
            Timestamp dataFine = null;
            
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
            
            if(dataInizio != null){
                Long giorni =  getDays(dataInizio, new Timestamp(new Date().getTime()));
                log.debug("Controllo consegna giorni differenza: " + giorni);
                if (giorni != null) {
                    
                    if(giorni > 1){ // Se il controlloSpedizione() è stato effettuato almeno un giorno fa 
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
            // Se la dataInizio è uguale a null la setto e ritorno true per far partire controlloSpedizione() al primo thread 
            // e setto canControllo per per i successivi
            setDataInizio();
            canStartControllo = true;
            return true;
        }
        return false;
    }
    
    private void setDataInizio() throws SQLException, NamingException{
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
    
    protected synchronized void setDataFine() throws SQLException, NamingException{
        numThread--;
        if (numThread == 0) {
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
