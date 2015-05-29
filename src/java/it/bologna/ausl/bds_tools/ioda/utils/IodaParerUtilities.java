package it.bologna.ausl.bds_tools.ioda.utils;

import it.bologna.ausl.ioda.iodaobjectlibrary.DatiParer;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.ioda.iodaoblectlibrary.exceptions.IodaDocumentException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.servlet.ServletContext;

/**
 *
 * @author gdm
 */
public class IodaParerUtilities {
    private String gdDocTable;
    private String aggiornamentiParerTable;

    private DatiParer datiParer;

    private IodaParerUtilities(ServletContext context) {
        this.gdDocTable = context.getInitParameter("GdDocsTableName");
        this.aggiornamentiParerTable = context.getInitParameter("AggiornamentiParerTableName");
    }

    /**
     * 
     * @param context
     * @param datiParer
     * @param prefixIds prefisso da anteporre agli id dei documenti che si ricercano (GdDoc)
     */
    public IodaParerUtilities(ServletContext context, DatiParer datiParer, String prefixIds) {
        this(context);
        this.datiParer = datiParer;
        datiParer.setPrefissoApplicazioneOrigine(prefixIds);
    }

    public void updateAggiornamentoParer(Connection dbConn) throws SQLException, IodaDocumentException {
        String xmlSpecifico = getXmlSpecificoGdDoc(dbConn);
        if (xmlSpecifico == null || xmlSpecifico.equals(""))
            throw new IodaDocumentException("Non è possibile fare l'update, xml specifico parer non presente");
        
        String idGdDoc = getIdGdDoc(dbConn);
        
        String sqlText =
                "INSERT INTO " + aggiornamentiParerTable + "(" +
                " id_gddoc, autore_aggiornamento, descrizione_aggiornamento, " +
                " xml_specifico_aggiornamento) " +
                "VALUES (" +
                "?, ?, ?, " +
                "?)";
                
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
            
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("Update non inserito");
        }
        finally {
            try {
                ps.close();
            }
            catch (Exception ex) {
            }
        }
        
        
    }

    public void insertAggiornamentoParer(Connection dbConn) throws SQLException, IodaDocumentException {
        String xmlSpecifico = getXmlSpecificoGdDoc(dbConn);
        if (xmlSpecifico != null && !xmlSpecifico.equals(""))
            throw new IodaDocumentException("Versamento già predisposto, impossibile cambiarlo");
        String sqlText =
                        "UPDATE " + gdDocTable + " SET " +
                        "xml_specifico_parer = ?, " +
                        "forza_conservazione = ?, " +
                        "forza_accettazione = ?, " +
                        "forza_collegamento = ? " +
                        "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        PreparedStatement ps = null;
        try {
            ps = dbConn.prepareStatement(sqlText);
            int index = 1;

            // xml_specifico_parer
            ps.setString(index++, datiParer.getXmlSpecifico());

            // forza_conservazione
            if (datiParer.getForzaConservazione() != null && datiParer.getForzaConservazione())    
                ps.setInt(index++, -1);
            else
                ps.setInt(index++, 0);

            // forza_accettazione
            if (datiParer.getForzaAccettazione()!= null && datiParer.getForzaAccettazione())    
                ps.setInt(index++, -1);
            else
                ps.setInt(index++, 0);

            // forza_collegamento
            if (datiParer.getForzaCollegamento()!= null && datiParer.getForzaCollegamento())    
                ps.setInt(index++, -1);
            else
                ps.setInt(index++, 0);

            ps.setString(index++, datiParer.getIdOggettoOrigine());
            ps.setString(index++, datiParer.getTipoOggettoOrigine());

            int res = ps.executeUpdate();
            if (res == 0)
               throw new SQLException("Documento non trovato");
        }
        finally {
            try {
                ps.close();
            }
            catch (Exception ex) {
            }
        }
    }
    
    private String getXmlSpecificoGdDoc(Connection dbConn) throws SQLException {
        String sqlText =
                        "SELECT xml_specifico_parer " +
                        "FROM " + gdDocTable + " " +
                        "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        PreparedStatement ps = null;
        try {
            String xmlSpecifico;
            ps = dbConn.prepareStatement(sqlText);
            ps.setString(1, datiParer.getIdOggettoOrigine());
            ps.setString(2, datiParer.getTipoOggettoOrigine());
            ResultSet res = ps.executeQuery();
            if (res.next() == false)
                throw new SQLException("Documento non trovato");
            else 
                xmlSpecifico = res.getString(1);
            return xmlSpecifico;
        }
        finally {
            try {
                ps.close();
            }
            catch (Exception ex) {
            }
        }
    }
    
    
    private String getIdGdDoc(Connection dbConn) throws SQLException {
        String sqlText =
                        "SELECT id_gddoc " +
                        "FROM " + gdDocTable + " " +
                        "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        PreparedStatement ps = null;
        try {
            String idGdDoc;
            ps = dbConn.prepareStatement(sqlText);
            ps.setString(1, datiParer.getIdOggettoOrigine());
            ps.setString(2, datiParer.getTipoOggettoOrigine());
            ResultSet res = ps.executeQuery();
            if (res.next() == false)
                throw new SQLException("Documento non trovato");
            else 
                idGdDoc = res.getString(1);
            return idGdDoc;
        }
        finally {
            try {
                ps.close();
            }
            catch (Exception ex) {
            }
        }
    }
    
}
