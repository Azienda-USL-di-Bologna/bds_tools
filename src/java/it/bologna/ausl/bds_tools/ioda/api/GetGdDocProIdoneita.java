package it.bologna.ausl.bds_tools.ioda.api;

import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.ioda.utils.IodaFascicoliUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicoli;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.IodaRequestDescriptor;
import it.bologna.ausl.ioda.iodaobjectlibrary.Researcher;
import it.bologna.ausl.mimetypeutility.Detector;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;


public class GetGdDocProIdoneita extends HttpServlet {

    private static final Logger log = LogManager.getLogger(GetGdDocProIdoneita.class);

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Connection dbConn = null;
        PreparedStatement ps = null;
        
        String limiteEstrazione = null;
        String idApplicazione = null;
        String tokenApplicazione = null;
        String numeroGiorniDaConsiderare = null;
        String codiceRegistro = null;
        String prefix;
        JSONArray result = new JSONArray();

        try {
            // dati per l'autenticazione
            idApplicazione = request.getParameter("idapplicazione");
            if (idApplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"idapplicazione\" mancante");
                return;
            }
            tokenApplicazione = request.getParameter("tokenapplicazione");
            if (tokenApplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"tokenapplicazione\" mancante");
                return;
            }
            
            numeroGiorniDaConsiderare = request.getParameter("giorni");
            if (numeroGiorniDaConsiderare == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"numeroGiorniDaConsiderare\" mancante");
                return;
            }
            
            codiceRegistro = request.getParameter("codiceregistro");
            if (codiceRegistro == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"codiceRegistro\" mancante");
                return;
            }
            
            limiteEstrazione = request.getParameter("limitestrazione");

            // ottengo una connessione al db
            try {
                dbConn = UtilityFunctions.getDBConnection();
            } catch (SQLException sQLException) {
                String message = "Problemi nella connesione al Database. Indicare i parametri corretti nei file di configurazione dell'applicazione" + "\n" + sQLException.getMessage();
                log.error(message);
                throw new ServletException(message);
            }

            // controllo se l'applicazione è autorizzata
            
            try {
                prefix = UtilityFunctions.checkAuthentication(dbConn, idApplicazione, tokenApplicazione);
            }
            catch (NotAuthorizedException ex) {
                try {
                    dbConn.close();
                }
                catch (Exception subEx) {
                }
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                return;
            }

            try{
                result = getArrayIDOggettoOrigine(codiceRegistro, numeroGiorniDaConsiderare, limiteEstrazione, prefix);
              
//              Researcher researcher = (Researcher) iodaReq.getObject();       
//              fascicoliUtilities = new IodaFascicoliUtilities(request, researcher);          
//              fascicoli = fascicoliUtilities.getFascicoli(dbConn, ps);

            }
            finally{
                if (ps != null)
                    ps.close();
                dbConn.close();
            }
            
           
        } catch (Exception ex) {
            throw new ServletException(ex);
        }

        response.setContentType(Detector.MEDIA_TYPE_APPLICATION_JSON.toString());
        try (PrintWriter out = response.getWriter()) {
            out.print(result.toJSONString());
        }
    }
    
    private JSONArray getArrayIDOggettoOrigine(String codiceRegistro, String numeroGiorniDaConsiderare, String limit, String prefix) throws SQLException, ServletException{
        
        JSONArray res = new JSONArray();
        
        String query = "SELECT g.id_oggetto_origine " +
                "FROM " + ApplicationParams.getDatiParerGdDocTableName() + " dp, " + ApplicationParams.getGdDocsTableName() + " g " + 
                "WHERE dp.id_gddoc = g.id_gddoc " + 
                "AND (dp.stato_versamento_effettivo = 'non_versare' or dp.stato_versamento_effettivo = 'errore_versamento') " + 
                "AND dp.xml_specifico_parer IS NOT NULL and dp.xml_specifico_parer !=  '' " + 
                "AND (dp.stato_versamento_proposto = 'da_versare' or dp.stato_versamento_proposto = 'da_aggiornare') " + 
                "AND g.codice_registro in (?) " + 
                "AND dp.idoneo_versamento = 0 " + 
                "AND g.data_registrazione >= current_date - ?::interval ";
        
        if (limit != null)
            query += "LIMIT " + limit;
        
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(query)
        ) {
            int index = 1;
            ps.setString(index++, codiceRegistro);   
            ps.setString(index++, numeroGiorniDaConsiderare + " day");

            String queryStr = ps.toString();
            log.debug("eseguo la query: " + queryStr + " ...");
            ResultSet resultQuery = ps.executeQuery();
            while (resultQuery.next()) {
                res.add(resultQuery.getString("id_oggetto_origine").replace(prefix, ""));
            }
        }
        catch(Exception ex) {
            throw new ServletException("Errore nel reperimento dei gddoc da inviare" + ex);
        }
        
        return res;
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}

