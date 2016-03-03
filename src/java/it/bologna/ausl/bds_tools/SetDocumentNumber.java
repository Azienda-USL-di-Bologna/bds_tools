package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;

/**
 *
 * @author Giuseppe De Marco (gdm)
 */
public class SetDocumentNumber extends HttpServlet {

private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(SetDocumentNumber.class);
    /** 
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    request.setCharacterEncoding("utf-8");
    // configuro il logger per la console
//    BasicConfigurator.configure();
    log.info("--------------------------------");
    log.info("Avvio servlet: " + getClass().getSimpleName());
    log.info("--------------------------------");

    Connection dbConn = null;
    

    String idapplicazione = null;
    String tokenapplicazione = null;
    String iddocumento = null;
    String nomesequenza = null;

        try {
            // leggo i parametri dalla richiesta HTTP

            // dati per l'autenticazione
            idapplicazione = request.getParameter("idapplicazione");
            tokenapplicazione = request.getParameter("tokenapplicazione");

            // dati per l'esecuzione della query
            iddocumento = request.getParameter("iddocumento");
            nomesequenza = request.getParameter("nomesequenza");

            log.debug("dati ricevuti:");
            log.debug("idapplicazione: " + idapplicazione);
            log.debug("iddocumento: " + iddocumento);
            log.debug("nomesequenza: " + nomesequenza);
            
            // controllo se mi sono stati passati i dati per l'autenticazione
            if (idapplicazione == null || tokenapplicazione == null) {
                String message = "Dati di autenticazione errati, specificare i parametri \"idapplicazione\" e \"tokenapplicazione\" nella richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            
            if (iddocumento == null || iddocumento.equals("")) {
                String message = "Dati errati, specificare il parametro \"iddocumento\" nella richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            
            if (nomesequenza == null || nomesequenza.equals("")) {
                String message = "Dati errati, specificare il parametro \"nomesequenza\" nella richiesta";
                log.error(message);
                throw new ServletException(message);
            }

            // ottengo una connessione al db
            try {
                dbConn = UtilityFunctions.getDBConnection();
            }
            catch (SQLException sQLException) {
                String message = "Problemi nella connesione al Data Base. Indicare i parametri corretti nei file di configurazione dell'applicazione" + "\n" + sQLException.getMessage();    
                log.error(message);
                throw new ServletException(message);
            }

            // controllo se l'applicazione è autorizzata
            String prefix;
            try {
                prefix = UtilityFunctions.checkAuthentication(dbConn, idapplicazione, tokenapplicazione);
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

            String number = setNumber(dbConn, getServletContext(), iddocumento, nomesequenza);

            response.setContentType("text/plain");
            try (PrintWriter out = response.getWriter()) {
                out.print(number);
            }
        }
        catch (Exception ex) {
            log.fatal("Errore", ex);
            log.info("dati ricevuti:");
            log.info("idapplicazione: " + idapplicazione);
            log.info("iddocumento: " + iddocumento);
            log.info("nomeregistro: " + nomesequenza);
            try {
                if (dbConn != null && dbConn.getAutoCommit() == false)
                    dbConn.rollback();
                if (dbConn != null)
                    dbConn.close();
            }
            catch (SQLException sQLException) {
                log.fatal("Errore nel rollback dell'operazione", sQLException);
            }
            throw new ServletException(ex);
        }
        finally {
            if (dbConn != null)
                try {
                    dbConn.close();
                }
                catch (SQLException ex) {
                    log.error("Errore nella chiusura della connessione", ex);
                }
        }
    }   
    
    public static String setNumber(Connection dbConn, ServletContext context, String idDocumento, String nomesequenza) throws ServletException, SQLException {

        String res = null;
        String updateNumberFunctionName = ApplicationParams.getUpdateNumberFunctionNameTemplate();
        updateNumberFunctionName = updateNumberFunctionName.replace("[nome_sequenza]", nomesequenza);

        if(updateNumberFunctionName == null || updateNumberFunctionName.equals("")) {
            String message = "Errore nel calcolo del nome della funzione che esegue l'aggiornamento del numero di documento";
            log.error(message);
            throw new ServletException(message);
        }

        // compongo la query
        dbConn.setAutoCommit(false);
        String sqlText = "select * from " + updateNumberFunctionName + "(?, ?)";
        PreparedStatement ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idDocumento);
        ps.setString(2, nomesequenza);
        try {
            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = ps.executeQuery();
            boolean nextRow = result.next();
            if (nextRow == false)
                throw new SQLException("errore nella numerazione");
            else {
                res = result.getString(1);
                
                try {
                    res = res + "/" + String.valueOf(result.getInt(2));
                }
                catch (Exception ex) {
                    log.warn("errore nella lettura dell'anno del protocollo, forse non c'è");
                }
                
                if (res == null || res.equals(""))
                    throw new SQLException("errore nella numerazione");
                dbConn.commit();
            }
        }
        finally {
            if (ps != null)
                ps.close();
        }
        return res;
    }
    
    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /** 
     * Handles the HTTP <code>GET</code> method.
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
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>
}