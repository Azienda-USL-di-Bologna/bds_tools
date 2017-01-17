package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.SetDocumentNumber;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author gdm
 */
@DisallowConcurrentExecution
public class DuplicatoreFascicoliAttivita implements Job{
    private static final Logger log = LogManager.getLogger(DuplicatoreFascicoliAttivita.class);
    private String sequenceName;
    
    private final int SQL_TRUE = -1;
    private final int SQL_FALSE = 0;
    private final String ID_APPLICAZIONE = "gedi";
    private final int TIPO_FASCICOLO_ATTIVITA = 3;

    private final String LIVELLO_FASCICOLO_RADICE = "1";
    private final String LIVELLO_FASCICOLO_SOTTOFASCICOLO = "2";
    private final String LIVELLO_FASCICOLO_INSERTO = "3";
    
    private final int MAX_LIVELLO_FASCICOLO = 3;

    private static final String[] COLUMN_TO_EXCLUDE = {"id_fascicolo","nome_fascicolo","numero_fascicolo","anno_fascicolo","id_fascicolo_padre",
        "numerazione_gerarchica","guid_fascicolo","stato_mestieri","in_uso_da","id_padre_catena_fascicolare","codice_fascicolo","tscol", "data_registrazione", 
        "id_fascicolo_importato", "data_chiusura", "note_importazione", "fascicolo_pregresso_collegato", "note",
        "copiato_da", "creato_dal_sistema"};
    
    private int currYear;

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        if (true)
            return;
        try (Connection dbConn = UtilityFunctions.getDBConnection();) {

            LocalDateTime now = LocalDateTime.now();
            currYear = now.getYear();
            try {
                if (notDoneThisYear(dbConn)) {
                    if (existFascicoloSpeciale(dbConn)) {
                        log.info(String.format("fascicolo speciale 1 dell'anno corrente %s trovato", currYear));
                        log.debug("=========================== Avvio Creazione Fascicoli Spaeciali ===========================");
                        updateDataInizioDataFine(dbConn, Timestamp.valueOf(now), null);
                        dbConn.setAutoCommit(false);
                        log.info("avvio duplicazione");
                        duplicateFascicoli(dbConn);
                        log.info("duplicazione terminata");
                        updateDataInizioDataFine(dbConn, null, Timestamp.valueOf(LocalDateTime.now()));
                        dbConn.commit();
                        log.debug("=========================== Fine Creazione Fascicoli Spaeciali ===========================");
                    }
                    else {
                        log.info("fascicolo speciale 1 dell'anno corrente NON trovato, impossibile procedere alla duplicazione");
                    }
                }
                else {
                    log.info(String.format("duplicazione fascicoli già eseguita per l'anno corrente: %s", currYear));
                }
            }
            catch (Exception ex) {
                log.error("Errore nella duplicazione dei fascicoli: ", ex);
                if (!dbConn.getAutoCommit())
                    dbConn.rollback();
                throw ex;
            }
        }
        catch (Exception ex) {
            log.error("Errore nel servizio di duplicazione dei fascicoli: ", ex);
        }
    }
    
    private boolean existFascicoloSpeciale(Connection dbConn) throws SQLException {
        String query = ""
                + "select id_fascicolo "
                + "from gd.fascicoligd "
                + "where numero_fascicolo = ? and "
                + "anno_fascicolo = ? and "
                + "speciale = ? and "
                + "id_livello_fascicolo = ?";
        
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            ps.setInt(1, 1);
            ps.setInt(2, currYear);
            ps.setInt(3, SQL_TRUE);
            ps.setString(4, LIVELLO_FASCICOLO_RADICE);
            
            log.info(String.format("verifico l'esistenza del fascicolo speciale con numero 1 dell'anno corrente(%s) ", currYear));
            log.debug(String.format("eseguo la query: %s...", ps.toString()));
            ResultSet res = ps.executeQuery();
            if (res.next()) 
                return true;
            else 
                return false;
        }
    }

    private boolean notDoneThisYear(Connection dbConn) throws SQLException {
        String query = ""
                + "select extract (year from data_inizio) "
                + "from bds_tools.servizi "
                + "where nome_servizio = ? and id_applicazione = ?";
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            ps.setString(1, getClass().getSimpleName());
            ps.setString(2, ID_APPLICAZIONE);
            log.debug(String.format("eseguo la query: %s", ps.toString()));
            ResultSet res = ps.executeQuery();
            if (res.next()) {
                int year = res.getInt(1);
                log.debug(String.format("anno letto: %s", year));
                return year < currYear;
            }
            else
                throw new SQLException(String.format("servizio %s dell'applicazione %s non trovato", getClass().getSimpleName(), ID_APPLICAZIONE));
        }
    }
    
    /**
     * aggiorna le date di avvio, termine del servizio, per aggiorarne solo una passare null all'altra
     * @param dataInizio
     * @param dataFine
     * @throws SQLException 
     */
    private void updateDataInizioDataFine(Connection dbConn, Timestamp dataInizio, Timestamp dataFine) throws SQLException {
        String query = ""
                + "update bds_tools.servizi "
                + "set "
                + "data_inizio = coalesce(?, data_inizio), "
                + "data_fine = coalesce(?, data_fine) "
                + "where nome_servizio = ? and id_applicazione = ?";
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            int index = 1;
            ps.setTimestamp(index++, dataInizio);
            ps.setTimestamp(index++, dataFine);
            ps.setString(index++, getClass().getSimpleName());
            ps.setString(index++, ID_APPLICAZIONE);
            
            log.debug(String.format("eseguo la query: %s...", ps.toString()));
            int res = ps.executeUpdate();
            if (res == 0)
                throw new SQLException("errore nell'aggiornamento delle date inizio/fine, l'update ha tornato 0");
        }
    }
    
    private ResultSet extractFascicoliAttivita(Connection dbConn, PreparedStatement ps, String livello) throws SQLException {
        String query = ""
                + "select * "
                + "from gd.fascicoligd "
                + "where id_livello_fascicolo = ? and "
                + "id_tipo_fascicolo = ? and "
                + "anno_fascicolo = ?";
        ps = dbConn.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ps.setString(1, livello);
        ps.setInt(2, TIPO_FASCICOLO_ATTIVITA);
        ps.setInt(3, currYear - 1);
        log.debug(String.format("eseguo la query %s...", ps.toString()));

        return ps.executeQuery();
    }
    
    private void duplicateFascicoli(Connection dbConn) throws SQLException, IOException, MalformedURLException, SendHttpMessageException, ServletException {
        //String query = "select * from gd.fascicoligd where guid_fascicolo in (?,?,?)";

        Map<String, String> idFascicoliOldNewMap1 = new HashMap();
        Map<String, String> idFascicoliOldNewMap2 = new HashMap();
        Map<String, String>[] idFascicoliMaps = new Map[2];
        idFascicoliMaps[0] = idFascicoliOldNewMap1;
        idFascicoliMaps[1] = idFascicoliOldNewMap2;

        for(int livello = 1; livello <= MAX_LIVELLO_FASCICOLO; livello++) {
            try (PreparedStatement ps = null) {
                ResultSet fascicoliRS = extractFascicoliAttivita(dbConn, ps, String.valueOf(livello));
                int fascicoliNumber = 0;
                if (fascicoliRS.last()) {
                    fascicoliNumber = fascicoliRS.getRow();
                    fascicoliRS.beforeFirst();
                    JSONArray indeIds = UtilityFunctions.getIndeId(fascicoliNumber);
                    int fascicoliCont = 0;
                    Map<String, String> readIdMap = idFascicoliMaps[(livello % 2)];
                    Map<String, String> writeIdMap = idFascicoliMaps[1 - (livello % 2)];
                    writeIdMap.clear();
                    while(fascicoliRS.next() && fascicoliCont < fascicoliNumber) {
                        fascicoliRS.moveToInsertRow();
                        JSONObject idAndGuid = (JSONObject)indeIds.get(fascicoliCont);
                        String idFascicoloSorgente = fascicoliRS.getString("id_fascicolo");
                        String idFascicoloDuplicato = (String) idAndGuid.get("document_id");
                        String guidFascicoloDuplicato = (String) idAndGuid.get("document_guid");
                        fascicoliRS.updateString("id_fascicolo", idFascicoloDuplicato);
                        
                        fascicoliRS.updateString("nome_fascicolo", null);
                        fascicoliRS.updateInt("anno_fascicolo", currYear);
                        fascicoliRS.updateString("guid_fascicolo", guidFascicoloDuplicato);
                        fascicoliRS.updateString("codice_fascicolo", fascicoliRS.getString("codice_fascicolo").replace(fascicoliRS.getString("guid_fascicolo"), guidFascicoloDuplicato));
                        fascicoliRS.updateString("copiato_da", idFascicoloSorgente);
                        fascicoliRS.updateInt("creato_dal_sistema", SQL_TRUE);

                        String idFascicoloPadre = null;
                        if (livello > 1) {
                            idFascicoloPadre = readIdMap.get(fascicoliRS.getString("id_fascicolo_padre"));
                            fascicoliRS.updateInt("numero_fascicolo", fascicoliRS.getInt("numero_fascicolo"));
                        }
                        else
                            fascicoliRS.updateInt("numero_fascicolo", 0);

                        if (livello < MAX_LIVELLO_FASCICOLO)
                            writeIdMap.put(idFascicoloSorgente, idFascicoloDuplicato);

                        fascicoliRS.updateString("id_fascicolo_padre", idFascicoloPadre);

                        ResultSetMetaData metaData = fascicoliRS.getMetaData();
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            String columnName = metaData.getColumnName(i);
                            int columnType = metaData.getColumnType(i);
                            if (!Arrays.stream(COLUMN_TO_EXCLUDE).anyMatch(c -> c.equalsIgnoreCase(columnName))) {
                                log.debug(String.format("processing column: %s of type: %s ",columnName, columnType));
                                fascicoliRS.updateObject(columnName, fascicoliRS.getObject(columnName));
                            }
                        }
                        fascicoliCont++;
                        fascicoliRS.insertRow();
                        fascicoliRS.moveToCurrentRow();
                        insertVicari(dbConn, idFascicoloSorgente, idFascicoloDuplicato);
                        
                        if (livello == 1) {
                            String number = SetDocumentNumber.setNumber(dbConn, guidFascicoloDuplicato, sequenceName);
                            log.info(String.format("numero generato: %s", number));
                        }
                    }
                }
            }
        }
    }

    private void insertVicari(Connection dbConn, String idFascicoloSorgente, String idFascicoloDuplicato) throws SQLException {
        
        String queryVicari = "select * from gd.fascicoli_gd_vicari where id_fascicolo = ?";
        try (PreparedStatement psVicari = dbConn.prepareStatement(queryVicari, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);) {
            psVicari.setString(1, idFascicoloSorgente);
            log.debug(String.format("eseguo la query: %s...", psVicari.toString()));
            ResultSet vicariRS = psVicari.executeQuery();
            if (vicariRS.last()) {
                int vicariNumber = vicariRS.getRow();
                log.debug(String.format("vicari tovati: ", vicariNumber));
                vicariRS.beforeFirst();
                int vicariCont = 0;
                while (vicariRS.next() && vicariCont < vicariNumber) {
                    log.debug(String.format("processing vicario row: %s --> %s", vicariRS.getString(1), vicariRS.getString(2)));
                    vicariRS.moveToInsertRow();
                    vicariRS.updateString("id_fascicolo", idFascicoloDuplicato);
                    vicariRS.updateString("id_utente", vicariRS.getString("id_utente"));
                    vicariRS.insertRow();
                    vicariCont++;
                    vicariRS.moveToCurrentRow();
                }
            }
            else {
                log.debug("non ci sono vicari");
            }
        }
    }
}