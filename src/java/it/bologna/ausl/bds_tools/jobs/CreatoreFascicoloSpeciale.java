package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.SetDocumentNumber;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.servlet.ServletException;
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
    private String sequenceName;
    private Connection dbConnection;

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

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    private void create() throws NullPointerException, SQLException, ServletException{
        idStrutturaUtenteResponsabile = getIdStrutturaResponsabile();
        // Fascicolo: Atti dell'azienda
        String idFascicoloSpecialeAtti = "FascicoloSpecial" + anno;
        // il numero fascicolo è a 0 perchè verra sovrascritto dalla storeprocidure che stacca i numeri e li assegna
        createFascicoloSpeciale(idFascicoloSpecialeAtti, 0, "1", fascicoloSpecialeAtti, null);
        // Sottofascicoli: Registri, Determinazioni, Deliberazioni
        String idFascicoloRegistro = "SFSpecialeRegis" + anno;
        createFascicoloSpeciale(idFascicoloRegistro, 1, "2", fascicoloSpecialeRegistri, idFascicoloSpecialeAtti);
        createFascicoloSpeciale("SFSpecialeDeter" + anno, 2, "2", fascicoloSpecialeDete, idFascicoloSpecialeAtti);
        createFascicoloSpeciale("SFSpecialeDeli" + anno, 3, "2", fascicoloSpecialeDeli, idFascicoloSpecialeAtti);
        // Ottengo id del sottofascicolo Registro
        
        if (idFascicoloRegistro == null || idFascicoloRegistro.equals("")) {
            throw new NullPointerException("Error: idFascicoloRegistro è null!");
        }
        // Inserti: Registro giornaliero di protocollo, Registro giornaliero delle determinazioni, Registro giornaliero delle deliberazioni, Registri annuali
        createFascicoloSpeciale("ISpecialeProto" + anno, 1, "3", registroGgProtocollo, idFascicoloRegistro);
        createFascicoloSpeciale("ISpecialeDeter" + anno, 2, "3", registroGgDeterminazioni, idFascicoloRegistro);
        createFascicoloSpeciale("ISpecialeDeli" + anno, 3, "3", registroGgDeliberazioni, idFascicoloRegistro);
        createFascicoloSpeciale("ISpecialeAnnuali" + anno, 4, "3", registriAnnuali, idFascicoloRegistro);
    }
    
    private void createFascicoloSpeciale(String idFascicolo, int numeroFascicolo, String livelloFascicolo, String nomeFascicoloSpeciale, String idFascicoloPadre) throws SQLException, ServletException{
        String queryCreateFascicolo = "INSERT INTO " + ApplicationParams.getFascicoliTableName() + "(" +
                                            "id_fascicolo, nome_fascicolo, numero_fascicolo, anno_fascicolo, " +
                                            "stato_fascicolo, id_livello_fascicolo, id_fascicolo_padre, " +
                                            "id_struttura, id_titolo, id_utente_responsabile, " +
                                            "id_utente_creazione, id_utente_responsabile_vicario, " +
                                            "data_creazione, data_responsabilita, id_tipo_fascicolo, codice_fascicolo, " +
                                            "eredita_permessi, speciale, guid_fascicolo, servizio_creazione) " +
                                            "VALUES (?, ?, ?, ?, ?, " +
                                                        "?, ?, ?, ?, " +
                                                        "?, ?, ?, ?, ?, " +
                                                        "?, ?, ?, ?, ?, ?);";
        try (
               PreparedStatement ps = dbConnection.prepareStatement(queryCreateFascicolo);
           ) {
            int i = 1;
            ps.setString(i++, idFascicolo);
            ps.setString(i++, nomeFascicoloSpeciale);
            // Nel caso sia il fascicolo "Atti" verra chiamata poi la funzione che stacchera e gli assegnera il numero
            if (idFascicoloPadre == null) {
                ps.setInt(i++, 0);
            }else{
                ps.setInt(i++, numeroFascicolo);
            }
            ps.setInt(i++, anno);
            ps.setString(i++, "a"); // "a" indica stato aperto
            ps.setString(i++, livelloFascicolo);
            ps.setString(i++, idFascicoloPadre);
            ps.setString(i++, idStrutturaUtenteResponsabile);
            ps.setString(i++, idClassificazioneFascicoloSpeciale);
            ps.setString(i++, idUtenteResponsabileFascicoloSpeciale);
            ps.setString(i++, idUtenteResponsabileFascicoloSpeciale);
            ps.setString(i++, idVicarioFascicoloSpeciale);// Molto probabilmente questo campo non è usato ma per sicurezza lo compiliamo
            ps.setDate(i++, new Date(Calendar.getInstance().getTimeInMillis()));
            ps.setDate(i++, new Date(Calendar.getInstance().getTimeInMillis()));
            ps.setInt(i++, 1);
            ps.setString(i++, "babel Codice" + idFascicolo);
            ps.setInt(i++, 0);
            ps.setInt(i++, -1);
            String guidFascicolo = "guid_" + idFascicolo;
            ps.setString(i++, guidFascicolo);
            ps.setString(i++, getClass().getSimpleName());
//            String numerazioneGerarchica = getNumerazioneGerarchica(idFascicoloPadre);
//             Caso fascicolo atti dell'azienda stacco il numero
//                E' stato introdotto un trigger che mette in automatico la numerazione gerarchica
            log.debug("QUERY INSERT: " + ps);
            ps.executeUpdate();
            if (idFascicoloPadre ==  null || idFascicoloPadre.equals("")) {
                SetDocumentNumber.setNumber(dbConnection, guidFascicolo, sequenceName);
            }
            setVicario(idFascicolo, idVicarioFascicoloSpeciale, dbConnection);
        } catch (SQLException ex) {
//            log.error("Errore nella creazione del fascicolo speciale: " + nomeFascicoloSpeciale, ex);
            throw new SQLException("Errore nella creazione del fascicolo speciale: " + nomeFascicoloSpeciale, ex);
        } catch (ServletException ex) {
//            log.error("Errore nell'assegnamento del numero al fascicolo: " + nomeFascicoloSpeciale, ex);
            throw new ServletException("Errore nell'assegnamento del numero al fascicolo: " + nomeFascicoloSpeciale, ex);
        }
    }
    
    private String getNumerazioneGerarchica(String idFascicolo) throws SQLException{
        if ( idFascicolo == null || idFascicolo.equals("")) {
            return null;
        }
        String queryNumerazioneGerarchica = "SELECT numerazione_gerarchica " +
                                                "FROM gd.fascicoligd " + 
                                                "WHERE id_fascicolo=?;";
        try (
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
        } catch (SQLException ex) {
//            log.error("Errore nel ottenimento della numerazione gerarchica del fascicolo: " + idFascicolo + " " + ex);
            throw new SQLException("Errore nel ottenimento della numerazione gerarchica del fascicolo: " + idFascicolo, ex);
        }
        return null;
    }
    
    private String getIdStrutturaResponsabile() throws SQLException{
        String queryIdStrutturaResponsabile = "SELECT id_struttura " +
                                                "FROM " + ApplicationParams.getUtentiTableName() + " " + 
                                                "WHERE id_utente=?;";
        try (
               PreparedStatement ps = dbConnection.prepareStatement(queryIdStrutturaResponsabile);
           ) {
            ps.setString(1, idUtenteResponsabileFascicoloSpeciale);
            ResultSet resIdStrutturaResponsabile = ps.executeQuery();
            while (resIdStrutturaResponsabile.next()) {                
                if (resIdStrutturaResponsabile != null && resIdStrutturaResponsabile.getString("id_struttura") != null && !resIdStrutturaResponsabile.getString("id_struttura").equals("")) {
                    return resIdStrutturaResponsabile.getString("id_struttura");
                }else{
                    throw new NullPointerException("La struttura del responsabile è null: " + resIdStrutturaResponsabile.getString("id_struttura"));
                }
            }
        } catch (SQLException ex) {
//            log.error("Errore nel ottenimento dell'id della struttura dell'utente responsabile: " + idUtenteResponsabileFascicoloSpeciale, ex);
            throw new SQLException("Errore nel ottenimento dell'id della struttura dell'utente responsabile: " + idUtenteResponsabileFascicoloSpeciale, ex);
        }
        return null;
    }
   
    private Integer getNumeroFascicolo(String idFascicolo) throws SQLException{
        if ( idFascicolo == null || idFascicolo.equals("")) {
            return null;
        }
        String queryNumeroFascicolo = "SELECT numero_fascicolo " +
                                                "FROM " + ApplicationParams.getFascicoliTableName() + " " + 
                                                "WHERE id_fascicolo=?;";
        try (
               PreparedStatement ps = dbConnection.prepareStatement(queryNumeroFascicolo);
           ) {
            ps.setString(1, idFascicolo);
            ResultSet resNumeroFascicolo = ps.executeQuery();
            while (resNumeroFascicolo.next()) {                
                if (resNumeroFascicolo.getInt("numero_fascicolo") != 0) {
                    return resNumeroFascicolo.getInt("numero_fascicolo");
                }else{
                    throw new NullPointerException("Il numero del fascicolo è null: " + resNumeroFascicolo.getInt("numero_fascicolo"));
                }
            }
        } catch (SQLException ex) {
//            log.error("Errore nel ottenimento del numero del fascicolo: " + idFascicolo, ex);
            throw new SQLException("Errore nel ottenimento del numero del fascicolo: " + idFascicolo, ex);
        }
        return null;
    }
    
    private String getIdFascicolo(String numerazioneGerarchica) throws SQLException{
         if ( numerazioneGerarchica == null || numerazioneGerarchica.equals("")) {
            return null;
        }
        String queryIdfascicolo = "SELECT id_fascicolo " +
                                                "FROM " + ApplicationParams.getFascicoliTableName() + " " + 
                                                "WHERE numerazione_gerarchica=?;";
        try (
               PreparedStatement ps = dbConnection.prepareStatement(queryIdfascicolo);
           ) {
            ps.setString(1, numerazioneGerarchica);
            log.debug("Query GetIdFascicolo: " + ps);
            ResultSet resIdFascicolo = ps.executeQuery();
            while (resIdFascicolo.next()) {                
                if (resIdFascicolo.getString("id_fascicolo") != null && !resIdFascicolo.getString("id_fascicolo").equals("")) {
                    return resIdFascicolo.getString("id_fascicolo");
                }else{
                    throw new NullPointerException("L'id del fascicolo è null: " + resIdFascicolo.getInt("id_fascicolo"));
                }
            }
        } catch (SQLException ex) {
//            log.error("Errore nel ottenimento dell'id del fascicolo: " + numerazioneGerarchica, ex);
            throw new SQLException("Errore nel ottenimento dell'id del fascicolo: " + numerazioneGerarchica, ex);
        }
        return null;
    }
    
    public static String setVicario(String idFascicolo, String idUtente, Connection connection) throws SQLException{
        if ( idFascicolo == null || idFascicolo.equals("")) {
            return null;
        }
        String querySetVicario = "INSERT INTO gd.fascicoli_gd_vicari (id_fascicolo,id_utente)" +
                                            "VALUES (?,?);";
        try (
//                PreparedStatement psSetVicario = dbConnection.prepareStatement(querySetVicario);
                PreparedStatement psSetVicario = connection.prepareStatement(querySetVicario);
           ) {
            psSetVicario.setString(1, idFascicolo);
//            log.debug("Query setVicario: " + psSetVicario);
            psSetVicario.executeUpdate();
        } catch (SQLException ex) {
            throw new SQLException("Errore nell'inserimento del vicario " + idUtente + " per il fascicolo " + idFascicolo, ex);
        }
        return null;
    }
    
    private Boolean alreadyExist() throws SQLException{
        String idFascicoloAttiAzienda = getIdFascicolo("1/" + anno);
        if(idFascicoloAttiAzienda != null && !idFascicoloAttiAzienda.equals("")){
            return true;
        }
        return false;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            dbConnection = UtilityFunctions.getDBConnection();
            dbConnection.setAutoCommit(false);
            if (!alreadyExist()) {
                log.debug("=========================== Avvio Creazione Fascicoli Spaeciali ===========================");
                create();
                dbConnection.commit();
                log.debug("=========================== Fine Creazione Fascicoli Spaeciali ===========================");
            }else{
                log.debug("ROLLBACK sulla transazione...");
                dbConnection.rollback();
                log.debug("FATTO!");
                log.debug("CLOSE della connessione...");
                dbConnection.close();
                log.debug("FATTO!");
//                log.debug("=========================== " + sequenceName + " ===========================");
                log.debug("=========================== I fascicoli speciali sono già presenti per l'anno corrente ===========================");
            }
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        } catch (Exception ex) {
            try {
                log.debug("ROLLBACK sulla transazione...");
                dbConnection.rollback();
                log.debug("FATTO!");
                log.debug("CLOSE della connessione...");
                dbConnection.close();
                log.debug("FATTO!");
            } catch (SQLException e) {
                log.debug("Errore durante il RollBack: ", e);
            }
            log.debug("Errore nella creazione dei fascicoli speciali: ", ex);
            log.debug("=========================== Creazione dei Fascicoli speciali FALLITA ===========================");
        }
    }
    
}
