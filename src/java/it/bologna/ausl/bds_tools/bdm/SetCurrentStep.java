package it.bologna.ausl.bds_tools.bdm;

import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
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

/**
 *
 * @author utente
 */
public class SetCurrentStep extends HttpServlet{
    
    private static final Logger log = LogManager.getLogger(SetCurrentStep.class);
    
    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    request.setCharacterEncoding("utf-8");
//     configuro il logger per la console
//    BasicConfigurator.configure();
    log.info("--------------------------------");
    log.info("Avvio servlet: " + getClass().getSimpleName());
    log.info("--------------------------------");
    Connection dbConn = null;
    PreparedStatement ps = null;
    String idapplicazione = null;
    String tokenapplicazione = null;
    
    String tipoOggetto = null;
    String idOggetto = null;
    String currentStep = null;

        try {
            // leggo i parametri dalla richiesta HTTP

            // dati per l'autenticazione
            idapplicazione = request.getParameter("idapplicazione");
            tokenapplicazione = request.getParameter("tokenapplicazione");
            
            // dati per l'esecuzione della query
            tipoOggetto = request.getParameter("tipooggetto");
            idOggetto = request.getParameter("idoggetto");
            currentStep = request.getParameter("currentstep");
            
            log.debug("dati ricevuti:");
            log.debug("idapplicazione: " + idapplicazione);
            log.debug("tipooggetto: " + tipoOggetto);
            log.debug("guiddocumento: " + idOggetto);
            log.debug("currentstep: " + currentStep);
            
            // controllo se mi sono stati passati i dati per l'autenticazione
            if (idapplicazione == null || tokenapplicazione == null) {
                String message = "Dati di autenticazione errati, specificare i parametri \"idapplicazione\" e \"tokenapplicazione\" nella richiesta";
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

            // leggo i parametri per l'esecuzione della query dal web.xml
            String setCurrentStepFunctionName = getServletContext().getInitParameter(tipoOggetto + "SetCurrentStepFunctionName");

            if(setCurrentStepFunctionName == null || setCurrentStepFunctionName.equals("")) {
                String message = "Manca il nome della funzione che scrive l'attivita sulla tabbella dei documenti. Indicarlo nel file \"web.xml\"";
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
            
            if(tipoOggetto == null || tipoOggetto.equals("")) {
                String message = "Manca il parametro \"tipoOggetto\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            else if(idOggetto == null || idOggetto.equals("")) {
                String message = "Manca il parametro \"idOggetto\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            else if(currentStep == null || currentStep.equals("")) {
                String message = "Manca il parametro \"currentstep\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            
            // compongo la query
            String sqlText = 
                    "select " + setCurrentStepFunctionName;

            ps = dbConn.prepareStatement(sqlText);
            int indexQuery = 1;
            ps.setString(indexQuery++, idOggetto);
            ps.setString(indexQuery++, currentStep);

            try {
                String query = ps.toString();
                log.debug("eseguo la query: " + query + " ...");
                boolean res = ps.execute();
//                if (res <= 0)
//                    throw new SQLException("Nessuna riga aggiornata");
                System.out.println(res);
            }
            catch (SQLException sQLException) {
                //dbConn.rollback();
                String message = "Errore nell'esecuzione della query";
                throw new ServletException(message, sQLException);
            }
            finally {
                if (ps != null)
                    ps.close();
                if (dbConn != null)
                    dbConn.close();
            }

            response.setContentType("text/html;charset=UTF-8");
            PrintWriter out = response.getWriter();
            try {
                out.println("<html>");
                out.println("<head>");
                out.println("<title>Servlet " + SetCurrentStep.class.getSimpleName() + "</title>");  
                out.println("</head>");
                out.println("<body>");
                out.println("<h1>Operazione eseguita correttamente</h1>");
                out.println("</body>");
                out.println("</html>");
            }
            finally {
                out.close();
            }
        }
        catch (Exception ex) {
            log.fatal("Errore", ex);
            log.info("dati ricevuti:");
            log.info("idapplicazione: " + idapplicazione);
            log.info("guiddocumento: " + idOggetto);
            log.info("currentstep: " + currentStep);

//            log.info("processinstanceuuid: " + processinstanceuuid);
            try {
                if (ps != null)
                    ps.close();
                if (dbConn != null)
                    dbConn.close();
            }
            catch (SQLException sQLException) {
                log.fatal("Errore nella chiusura delle connessioni al db", sQLException);
            }
            throw new ServletException(ex);
        }
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
