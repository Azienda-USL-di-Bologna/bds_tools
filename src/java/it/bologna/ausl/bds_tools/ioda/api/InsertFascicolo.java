package it.bologna.ausl.bds_tools.ioda.api;

import com.fasterxml.jackson.databind.JsonMappingException;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaFascicoliUtilities;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.IodaRequestDescriptor;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InsertFascicolo extends HttpServlet {

    private static final Logger log = LogManager.getLogger(InsertFascicolo.class);

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
        request.setCharacterEncoding("utf-8");

        // configuro il logger per la console
        log.info("--------------------------------");
        log.info("Avvio servlet: " + getClass().getSimpleName());
        log.info("--------------------------------");

        IodaFascicoliUtilities fascicoliUtilities;
        Connection dbConn = null;
        PreparedStatement ps = null;
        Fascicolo fascicoloResult;
        try {
            // leggo i parametri dalla richiesta
            IodaRequestDescriptor iodaRequest;
            try (InputStream is = request.getInputStream()) {
                if (is == null) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "json della richiesta mancante");
                    return;
                }
                iodaRequest = IodaRequestDescriptor.parse(is);
            } catch (Exception ex) {
                log.error("formato json della richiesta errato: " + ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "formato json della richiesta errato: " + ex);
                return;
            }

            // dati per l'autenticazione
            String idapplicazione = iodaRequest.getIdApplicazione();
            String tokenapplicazione = iodaRequest.getTokenApplicazione();

            // ottengo una connessione al db
            try {
                dbConn = UtilityFunctions.getDBConnection();
            } catch (SQLException sQLException) {
                String message = "Problemi nella connesione al Data Base. Indicare i parametri corretti nei file di configurazione dell'applicazione" + "\n" + sQLException.getMessage();
                log.error(message);
                throw new ServletException(message);
            }

            // controllo se l'applicazione è autorizzata
            String prefix;

            try {
                prefix = UtilityFunctions.checkAuthentication(dbConn, idapplicazione, tokenapplicazione);
            } catch (NotAuthorizedException ex) {
                try {
                    dbConn.close();
                } catch (Exception subEx) {
                }
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                return;
            }

            Fascicolo fascicolo;
            try {
                fascicolo = (Fascicolo) iodaRequest.getObject();
                fascicoliUtilities = new IodaFascicoliUtilities(request, fascicolo);
            } catch (IodaDocumentException | JsonMappingException ex) {
                try {
                    dbConn.close();
                } catch (Exception subEx) {
                }
                log.error(ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }

            try {
                dbConn.setAutoCommit(false);
                String guidFascicoloCreato = fascicoliUtilities.insertFascicolo(dbConn, iodaRequest.getAdditionalData());

                dbConn.commit();

                fascicoloResult = fascicoliUtilities.getFascicolo(dbConn, guidFascicoloCreato);

            } catch (Exception ex) {
                log.error("errore nella gestione dell'inserimento di un nuovo fascicolo: ", ex);
                dbConn.rollback();
                throw ex;
            } finally {
                if (ps != null) {
                    ps.close();
                }
                dbConn.close();
            }
        } catch (Exception ex) {
            log.error("errore nella gestione dell'inserimento di un nuovo fascicolo: ", ex);
            throw new ServletException(ex);
        }

        response.setContentType("text/html;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            out.print(fascicoloResult.getJSONString());
        } catch (Exception ex) {
            log.error("errore della servlet: ", ex);
        }
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
        return "Inserisce un Fascicolo in GEDI";
    }// </editor-fold>

}
