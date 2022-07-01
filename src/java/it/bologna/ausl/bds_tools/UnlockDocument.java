package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.ioda.utils.LockUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.springframework.util.StringUtils;

/**
 *
 * @author gdm
 */
public class UnlockDocument extends HttpServlet {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(UnlockDocument.class);
    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        log.info("--------------------------------");
        log.info("Avvio servlet: " + getClass().getSimpleName());
        log.info("--------------------------------");
        
        String guid = null;
        String userId = null;
        String step = null;
        String ambito = null;

        try {
            guid = request.getParameter("guid");
            userId = request.getParameter("userId");
            step = request.getParameter("step");
            ambito = request.getParameter("ambito");
            
            log.info("dati ricevuti:");
            log.info("guid: " + guid);
            log.info("userId: " + userId);
            log.info("step: " + step);
            log.info("ambito: " + ambito);
            
            if (!StringUtils.hasText(guid) || !StringUtils.hasText(userId) || !StringUtils.hasText(step) || !StringUtils.hasText(ambito)) {
                log.error("manca almeno un parametro");
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "manca almeno un parametro");
                return;
            }
            switch (step) {
                case "PARERI":
                    log.info("unlocking PARERI for user: " + userId);
                    LockUtilities lockUtilities = new LockUtilities(guid, "DocumentoPico", userId, ApplicationParams.getServerId(), "ambito_firma", ApplicationParams.getRedisHost());
                    log.info("removing lock key: " + lockUtilities.getKey());
                    lockUtilities.removeUserLock();
                    break;
                case "FIRMA": 
                    break;
                case "Smistamento":
                case "SEGRETERIA":
                case "RESPONSABILE":
                case "ASSEGNAZIONE":
                    log.info("unlocking Smistamento/i for user: " + userId);
                    
                    String query = "update bds_tools.smistamenti set id_utente_lock = null where guid_documento = ? and id_utente_lock = ?";
                    PreparedStatement ps = null;
                    try (Connection dbConn = UtilityFunctions.getDBConnection();) {
                        ps = dbConn.prepareStatement(query);
                        ps.setString(1, guid);
                        ps.setString(2, userId);
                        log.info("execute query: " + ps.toString());
                        ps.executeUpdate();
                    } catch (Throwable t) {
                        log.error("errore nell'esecuzione della query", t);
                    } finally {
                        try {
                        ps.close();
                        } catch (Exception ex) {
                            log.error("errore when closing PreparedStatement", ex);
                        }
                    }
                    
                    break;

            }
            
        } catch (Exception ex) {
            log.error("errore:", ex);
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
