/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import javax.naming.NamingException;
import org.apache.logging.log4j.LogManager;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author Fayssel
 */
@DisallowConcurrentExecution
public class CreatoreFascicoloSpeciale implements Job{
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(CreatoreFascicoloSpeciale.class);
    private final String fascicoloSpecialeAtti;
    private final String fascicoloSpecialeRegistri;
    private final String fascicoloSpecialeDete;
    private final String fascicoloSpecialeDeli;
    private final String idUtenteResponsabileFascicoloSpeciale;
    private final String idVicarioFascicoloSpeciale;
    private final String idClassificazioneFascicoloSpeciale;    
    private final String registroGgProtocollo;
    private final String registroGgDeliberazioni;
    private final String registroGgDeterminazioni;
    private final String registriAnnuali;
    private final Integer anno;
    private String idStrutturaUtenteResponsabile;

    public CreatoreFascicoloSpeciale() {
        log.debug("=========================== Inizializzazione Servizio Creazione Fascicoli Speciali ===========================");
        idUtenteResponsabileFascicoloSpeciale  = ApplicationParams.getOtherPublicParam("idUtenteResponsabileFascicoloSpeciale");
        idVicarioFascicoloSpeciale  = ApplicationParams.getOtherPublicParam("idVicarioFascicoloSpeciale");
        idClassificazioneFascicoloSpeciale = ApplicationParams.getOtherPublicParam("idClassificazioneFascSpeciale");
        String nomeFascicoli = ApplicationParams.getOtherPublicParam("nomeFascicoliSpeciali");
        String[] nomeFascicoliSplittati = nomeFascicoli.split(":");
        int i = 0;
        fascicoloSpecialeAtti = nomeFascicoliSplittati[i++];
        fascicoloSpecialeRegistri = nomeFascicoliSplittati[i++];
        fascicoloSpecialeDete = nomeFascicoliSplittati[i++];
        fascicoloSpecialeDeli = nomeFascicoliSplittati[i++];
        registroGgProtocollo = nomeFascicoliSplittati[i++];
        registroGgDeterminazioni = nomeFascicoliSplittati[i++];
        registroGgDeliberazioni = nomeFascicoliSplittati[i++];
        registriAnnuali = nomeFascicoliSplittati[i++];
        anno = Calendar.getInstance().get(Calendar.YEAR);
        log.debug("=========================== Fine Inizializzazione Servizio Creazione Fascicoli Speciali ===========================");
    }

    public void create() throws Exception{
        idStrutturaUtenteResponsabile = getIdStrutturaResponsabile();
        // Fascicolo: Atti dell'azienda
        String idFascicoloSpecialeAtti = "FascicoloSpecial" + anno;
        createFascicoloSpeciale(idFascicoloSpecialeAtti, 1, "1", fascicoloSpecialeAtti, null);
        // Sottofascicoli: Registri, Determinazioni, Deliberazioni
        createFascicoloSpeciale("Special2016" + anno, 1, "2", fascicoloSpecialeRegistri, idFascicoloSpecialeAtti);
        createFascicoloSpeciale("SFSpecialeDeter" + anno, 2, "2", fascicoloSpecialeDete, idFascicoloSpecialeAtti);
        createFascicoloSpeciale("SFSpecialeDeli" + anno, 3, "2", fascicoloSpecialeDeli, idFascicoloSpecialeAtti);
        // Ottengo id del sottofascicolo Registro
        String idFascicoloRegistro = getIdFascicolo("1-1/" + anno);
        if (idFascicoloRegistro == null || idFascicoloRegistro.equals("")) {
            throw new Exception("Error: idFascicoloRegistro è null!");
        }
        // Inserti: Registro giornaliero di protocollo, Registro giornaliero delle determinazioni, Registro giornaliero delle deliberazioni, Registri annuali
        createFascicoloSpeciale("ISpecialeProto" + anno, 1, "3", registroGgProtocollo, idFascicoloRegistro);
        createFascicoloSpeciale("ISpecialeDeter" + anno, 2, "3", registroGgDeterminazioni, idFascicoloRegistro);
        createFascicoloSpeciale("ISpecialeDeli" + anno, 3, "3", registroGgDeliberazioni, idFascicoloRegistro);
        createFascicoloSpeciale("ISpecialeAnnuali" + anno, 4, "3", registriAnnuali, idFascicoloRegistro);
    }
    
    private void createFascicoloSpeciale(String idFascicolo, int numeroFascicolo, String livelloFascicolo, String nomeFascicoloSpeciale, String idFascicoloPadre){
        String queryCreateFascicolo = "INSERT INTO " + ApplicationParams.getFascicoliTableName() + "(" +
                                            "id_fascicolo, nome_fascicolo, numero_fascicolo, anno_fascicolo, " +
                                            "stato_fascicolo, id_livello_fascicolo, id_fascicolo_padre, " +
                                            "id_struttura, id_titolo, id_utente_responsabile, " +
                                            "id_utente_creazione, id_utente_responsabile_vicario, " +
                                            "data_creazione, data_responsabilita, id_tipo_fascicolo, codice_fascicolo, " +
                                            "eredita_permessi, speciale, numerazione_gerarchica) " +
                                            "VALUES (?, ?, ?, ?, ?, " +
                                                        "?, ?, ?, ?, " +
                                                        "?, ?, ?, ?, ?, " +
                                                        "?, ?, ?, ?, ?);";
        try (
               Connection dbConnection = UtilityFunctions.getDBConnection();
               PreparedStatement ps = dbConnection.prepareStatement(queryCreateFascicolo);
           ) {
            int i = 1;
            ps.setString(i++, idFascicolo);
            ps.setString(i++, nomeFascicoloSpeciale);
            ps.setInt(i++, numeroFascicolo);
            ps.setInt(i++, anno);
            ps.setString(i++, "a"); // "a" indica stato aperto
            ps.setString(i++, livelloFascicolo);
            ps.setString(i++, idFascicoloPadre);
            ps.setString(i++, idStrutturaUtenteResponsabile);
            ps.setString(i++, idClassificazioneFascicoloSpeciale);
            ps.setString(i++, idUtenteResponsabileFascicoloSpeciale);
            ps.setString(i++, idUtenteResponsabileFascicoloSpeciale);
            ps.setString(i++, idVicarioFascicoloSpeciale);
            ps.setDate(i++, new Date(Calendar.getInstance().getTimeInMillis()));
            ps.setDate(i++, new Date(Calendar.getInstance().getTimeInMillis()));
            ps.setInt(i++, 1);
            ps.setString(i++, "babel Codice" + idFascicolo);
            ps.setInt(i++, 0);
            ps.setInt(i++, -1);
            String numerazioneGerarchica = getNumerazioneGerarchica(idFascicoloPadre);
            if (numerazioneGerarchica ==  null || numerazioneGerarchica.equals("")) {
                ps.setString(i++, numeroFascicolo + "/" + anno);
            }else{
                ps.setString(i++, numerazioneGerarchica + "-" + numeroFascicolo + "/" + anno);
            }
            log.debug("QUERY INSERT: " + ps);
            ps.executeUpdate();
        } catch (SQLException | NamingException ex) {
            log.error("Errore nella creazione del fascicolo speciale: " + nomeFascicoloSpeciale, ex);
        }
    }
    
    private String getNumerazioneGerarchica(String idFascicolo){
        if ( idFascicolo == null || idFascicolo.equals("")) {
            return null;
        }
        String queryNumerazioneGerarchica = "SELECT numerazione_gerarchica " +
                                                "FROM gd.fascicoligd " + 
                                                "WHERE id_fascicolo=?;";
        try (
               Connection dbConnection = UtilityFunctions.getDBConnection();
               PreparedStatement psNumerazioneGerarchica = dbConnection.prepareStatement(queryNumerazioneGerarchica);
           ) {
            psNumerazioneGerarchica.setString(1, idFascicolo);
            ResultSet resNumeroFascicolo = psNumerazioneGerarchica.executeQuery();
            while (resNumeroFascicolo.next()) {                
                if (!resNumeroFascicolo.getString("numerazione_gerarchica").equals("")) {
                    String numerazioneGerarchica = resNumeroFascicolo.getString("numerazione_gerarchica");
                    String[] splitted = numerazioneGerarchica.split("/");
                    return splitted[0];
                }else{
                    throw new NullPointerException("La numerazione gerarchica del fascicolo è null: " + resNumeroFascicolo.getInt("numerazione_gerarchica"));
                }
            }
        } catch (SQLException | NamingException ex) {
            log.error("Errore nel ottenimento della numerazione gerarchica del fascicolo: " + idFascicolo + " " + ex);
        }
        return null;
    }
    
    private String getIdStrutturaResponsabile(){
        String queryIdStrutturaResponsabile = "SELECT id_struttura " +
                                                "FROM " + ApplicationParams.getUtentiTableName() + " " + 
                                                "WHERE id_utente=?;";
        try (
               Connection dbConnection = UtilityFunctions.getDBConnection();
               PreparedStatement ps = dbConnection.prepareStatement(queryIdStrutturaResponsabile);
           ) {
            ps.setString(1, idUtenteResponsabileFascicoloSpeciale);
            ResultSet resIdStrutturaResponsabile = ps.executeQuery();
            while (resIdStrutturaResponsabile.next()) {                
                if (resIdStrutturaResponsabile != null && resIdStrutturaResponsabile.getString("id_struttura") != null && !resIdStrutturaResponsabile.getString("id_struttura").equals("")) {
                    return resIdStrutturaResponsabile.getString("id_struttura");
                }else{
                    throw new Exception("La struttura del responsabile è null: " + resIdStrutturaResponsabile.getString("id_struttura"));
                }
            }
        } catch (SQLException | NamingException ex) {
            log.error("Errore nel ottenimento dell'id della struttura dell'utente responsabile: " + idUtenteResponsabileFascicoloSpeciale, ex);
        } catch (Exception ex) {
            log.error(ex);
        }
        return null;
    }
   
    private Integer getNumeroFascicolo(String idFascicolo){
        if ( idFascicolo == null || idFascicolo.equals("")) {
            return null;
        }
        String queryNumeroFascicolo = "SELECT numero_fascicolo " +
                                                "FROM " + ApplicationParams.getFascicoliTableName() + " " + 
                                                "WHERE id_fascicolo=?;";
        try (
               Connection dbConnection = UtilityFunctions.getDBConnection();
               PreparedStatement ps = dbConnection.prepareStatement(queryNumeroFascicolo);
           ) {
            ps.setString(1, idFascicolo);
            ResultSet resNumeroFascicolo = ps.executeQuery();
            while (resNumeroFascicolo.next()) {                
                if (resNumeroFascicolo.getInt("numero_fascicolo") != 0) {
                    return resNumeroFascicolo.getInt("numero_fascicolo");
                }else{
                    throw new Exception("Il numero del fascicolo è null: " + resNumeroFascicolo.getInt("numero_fascicolo"));
                }
            }
        } catch (SQLException | NamingException ex) {
            log.error("Errore nel ottenimento del numero del fascicolo: " + idFascicolo, ex);
        } catch (Exception ex) {
            log.error(ex);
        }
        return null;
    }
    
    private String getIdFascicolo(String numerazioneGerarchica){
         if ( numerazioneGerarchica == null || numerazioneGerarchica.equals("")) {
            return null;
        }
        String queryIdfascicolo = "SELECT id_fascicolo " +
                                                "FROM " + ApplicationParams.getFascicoliTableName() + " " + 
                                                "WHERE numerazione_gerarchica=?;";
        try (
               Connection dbConnection = UtilityFunctions.getDBConnection();
               PreparedStatement ps = dbConnection.prepareStatement(queryIdfascicolo);
           ) {
            ps.setString(1, numerazioneGerarchica);
            ResultSet resIdFascicolo = ps.executeQuery();
            while (resIdFascicolo.next()) {                
                if (resIdFascicolo.getString("id_fascicolo") != null && !resIdFascicolo.getString("id_fascicolo").equals("")) {
                    return resIdFascicolo.getString("id_fascicolo");
                }else{
                    throw new Exception("L'id del fascicolo è null: " + resIdFascicolo.getInt("id_fascicolo"));
                }
            }
        } catch (SQLException | NamingException ex) {
            log.error("Errore nel ottenimento dell'id del fascicolo: " + numerazioneGerarchica, ex);
        } catch (Exception ex) {
            log.error(ex);
        }
        return null;
    }
    
    private Boolean alreadyExist(){
        String idFascicoloAttiAzienda = getIdFascicolo("1/" + anno);
        if(idFascicoloAttiAzienda != null && !idFascicoloAttiAzienda.equals("")){
            return true;
        }
        return false;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        
//        if(true)
//            return;
        
        try {
            if (!alreadyExist()) {
                log.debug("=========================== Avvio Creazione Fascicoli Spaeciali ===========================");
                create(); 
                log.debug("=========================== Fine Creazione Fascicoli Spaeciali ===========================");
            }else{
                log.debug("=========================== I fascicoli speciali sono già presenti per l'anno corrente ===========================");
            }
        } catch (Exception ex) {
            log.error("Errore nella creazione dei fascicoli: ", ex);
        }
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
