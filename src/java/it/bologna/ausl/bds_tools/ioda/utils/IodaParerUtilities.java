package it.bologna.ausl.bds_tools.ioda.utils;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.ioda.iodaobjectlibrary.DatiParer;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author gdm
 */
public class IodaParerUtilities {

    private static final Logger log = LogManager.getLogger(IodaParerUtilities.class);

    private String gdDocTable;
    private String aggiornamentiParerTable;

    private DatiParer datiParer;

    private IodaParerUtilities() {
        this.gdDocTable = ApplicationParams.getGdDocsTableName();
        this.aggiornamentiParerTable = ApplicationParams.getAggiornamentiParerTableName();
    }

    /**
     *
     * @param datiParer
     * @param prefixIds prefisso da anteporre agli id dei documenti che si
     * ricercano (GdDoc)
     */
    public IodaParerUtilities(DatiParer datiParer, String prefixIds) {
        this();
        this.datiParer = datiParer;
        datiParer.setPrefissoApplicazioneOrigine(prefixIds);
    }

    public void updateAggiornamentoParer(Connection dbConn) throws SQLException, IodaDocumentException {
        String xmlSpecifico = getXmlSpecificoGdDoc(dbConn);
        if (xmlSpecifico == null || xmlSpecifico.equals("")) {
            throw new IodaDocumentException("Non è possibile fare l'update, xml specifico parer non presente");
        }

        String idGdDoc = getIdGdDoc(dbConn);

        String sqlText
                = "INSERT INTO " + aggiornamentiParerTable + "("
                + " id_gddoc, autore_aggiornamento, descrizione_aggiornamento, "
                + " xml_specifico_aggiornamento) "
                + "VALUES ("
                + "?, ?, ?, "
                + "?)";

        PreparedStatement ps = null;

        try {
            ps = dbConn.prepareStatement(sqlText);
            int index = 1;

            // id_gddoc
            ps.setString(index++, idGdDoc);

            // autore aggiornamento
            ps.setString(index++, datiParer.getAutoreAggiornamento());

            // descrizione aggiornamento
            ps.setString(index++, datiParer.getDescrizioneAggiornamento());

            // xml specifico aggiornamento
            ps.setString(index++, datiParer.getXmlSpecifico());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");

            int result = ps.executeUpdate();
            log.debug("fatto");

            if (result <= 0) {
                throw new SQLException("Update non inserito");
            }
        } finally {
            try {
                ps.close();
            } catch (Exception ex) {
            }
        }

    }

    public void insertAggiornamentoParer(Connection dbConn) throws SQLException, IodaDocumentException {

//        String xmlSpecifico = getXmlSpecificoGdDoc(dbConn);
//      controllo da decommentare quando avremo un'idea chiara circa gli aggiornamenti al ParER  
//        if (xmlSpecifico != null && !xmlSpecifico.equals(""))
//            throw new IodaDocumentException("Versamento già predisposto, impossibile cambiarlo");
        String sqlText
                = "UPDATE " + gdDocTable + " SET "
                + "xml_specifico_parer = ?, "
                + "forza_conservazione = ?, "
                + "forza_accettazione = ?, "
                + "forza_collegamento = ?, "
                + "stato_versamento_proposto = ? "
                + "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        PreparedStatement ps = null;
        try {
            log.info("Dentro il try: Preparo lo statement...");
            ps = dbConn.prepareStatement(sqlText);
            int index = 1;

            // xml_specifico_parer
            ps.setString(index++, datiParer.getXmlSpecifico());

            // forza_conservazione
            if (datiParer.getForzaConservazione() != null && datiParer.getForzaConservazione()) {
                ps.setInt(index++, -1);
            } else {
                ps.setInt(index++, 0);
            }

            // forza_accettazione
            if (datiParer.getForzaAccettazione() != null && datiParer.getForzaAccettazione()) {
                ps.setInt(index++, -1);
            } else {
                ps.setInt(index++, 0);
            }

            // forza_collegamento
            if (datiParer.getForzaCollegamento() != null && datiParer.getForzaCollegamento()) {
                ps.setInt(index++, -1);
            } else {
                ps.setInt(index++, 0);
            }

            ps.setString(index++, datiParer.getStatoVersamentoProposto());

            ps.setString(index++, datiParer.getIdOggettoOrigine());
            ps.setString(index++, datiParer.getTipoOggettoOrigine());

            String query = ps.toString();
            log.info("(try) eseguo la query: " + query + " ...");

            int res = ps.executeUpdate();

            log.info("fatto (affected: " + res + ")");

            if (res == 0) {
                log.error("Nessuna riga affected: allora rilancio SQLException");
                throw new SQLException("Documento non trovato");
            }
        } finally {
            try {
                log.info("(finally) Chiudo il preparedStatement");
                ps.close();
                log.info("fatto.");
            } catch (Exception ex) {
                log.error("Eccezione catchata non rilanciata", ex);
            }
        }
        log.info("(Fuori dal finally) Tutto finito");
    }

    private String getXmlSpecificoGdDoc(Connection dbConn) throws SQLException {
        String sqlText
                = "SELECT xml_specifico_parer "
                + "FROM " + gdDocTable + " "
                + "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        PreparedStatement ps = null;
        try {
            log.info("(try) preparo lo statement...");
            String xmlSpecifico;
            ps = dbConn.prepareStatement(sqlText);
            ps.setString(1, datiParer.getIdOggettoOrigine());
            ps.setString(2, datiParer.getTipoOggettoOrigine());

            String query = ps.toString();
            log.info("eseguo la query: " + query + " ...");

            ResultSet res = ps.executeQuery();
            log.info("fatto");

            if (res.next() == false) {
                log.error("Errore, non ho risultati: rilancio SQLException");
                throw new SQLException("Documento non trovato");
            } else {
                log.info("Setto xmlSpecifico");
                xmlSpecifico = res.getString(1);
            }
            log.info("Ritorno xmlSpecifico");
            return xmlSpecifico;
        } finally {
            try {
                log.info("Chiudo il prepared statement");
                ps.close();
            } catch (Exception ex) {
                log.error("Eccezzione non rilanciata", ex);
            }
        }
    }

    private String getIdGdDoc(Connection dbConn) throws SQLException {
        String sqlText
                = "SELECT id_gddoc "
                + "FROM " + gdDocTable + " "
                + "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        PreparedStatement ps = null;
        try {
            String idGdDoc;
            ps = dbConn.prepareStatement(sqlText);
            ps.setString(1, datiParer.getIdOggettoOrigine());
            ps.setString(2, datiParer.getTipoOggettoOrigine());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");

            ResultSet res = ps.executeQuery();
            log.debug("fatto");

            if (res.next() == false) {
                throw new SQLException("Documento non trovato");
            } else {
                idGdDoc = res.getString(1);
            }
            return idGdDoc;
        } finally {
            try {
                ps.close();
                log.info("Prepared statement chiuso correttamente");
            } catch (Exception ex) {
                log.error("Eccezione non rilanciata", ex);
            }
        }
    }

}
