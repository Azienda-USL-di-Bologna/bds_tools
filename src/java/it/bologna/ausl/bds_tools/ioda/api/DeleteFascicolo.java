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
import java.util.HashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Sal The Great
 */
public class DeleteFascicolo extends HttpServlet {

    private static final Logger log = LogManager.getLogger(DeleteFascicolo.class);

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

        log.info("--------------------------------");
        log.info("Avvio servlet: " + getClass().getSimpleName());
        log.info("--------------------------------");

        IodaFascicoliUtilities fascicoliUtilities;
        IodaRequestDescriptor iodaReq;
        Connection dbConn = null;
        PreparedStatement ps = null;
        Boolean fascicoloCancellato;  // da sistemare

        try {

            try (InputStream is = request.getInputStream()) {
                if (is == null) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "json della richiesta mancante");
                    return;
                }
                iodaReq = IodaRequestDescriptor.parse(is);
            } catch (Exception ex) {
                log.error("formato json della richiesta errato: " + ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "formato json della richiesta errato: " + ex);
                return;
            }
            // dati per l'autenticazione
            String idapplicazione = iodaReq.getIdApplicazione();
            if (idapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"idapplicazione\" mancante");
                return;
            }
            String tokenapplicazione = iodaReq.getTokenApplicazione();
            if (tokenapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"tokenapplicazione\" mancante");
                return;
            }

            // ottengo una connessione al db
            try {
                dbConn = UtilityFunctions.getDBConnection();
            } catch (SQLException sQLException) {
                String message = "Problemi nella connesione al Database. Indicare i parametri corretti nei file di configurazione dell'applicazione" + "\n" + sQLException.getMessage();
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
                fascicolo = (Fascicolo) iodaReq.getObject();
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
                HashMap additionalData = (HashMap) iodaReq.getAdditionalData();
                // la mappa deve essere presa in realtà con iodaReq.getAdditionalData();
                fascicoloCancellato = fascicoliUtilities.deleteFascicolo(dbConn, ps, additionalData);

            } catch (Exception ex) {
                log.error("errore nella cancellazione del fascicolo: ", ex);
                throw ex;
            } finally {
                if (ps != null) {
                    ps.close();
                }
                dbConn.close();
            }

        }  catch (Throwable ex) {
            log.error("errore", ex);
            throw new ServletException(ex);
        }
        finally {
            try {
                ps.close();
            } catch (Exception subEx) {
                log.warn("errore nella chiusura della connessione", subEx);
            }
            try {
                dbConn.close();
            } catch (Exception subEx) {
                log.warn("errore nella chiusura della connessione", subEx);
            }
        }

        response.setContentType("text/html;charset=UTF-8");
        response.addHeader("fascicoloCancellato", fascicoloCancellato ? "true" : "false");
        try (PrintWriter out = response.getWriter()) {
            out.print(fascicoloCancellato);
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
        return "Short description";
    }// </editor-fold>

}
