package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
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
 * @author Giuseppe De Marco (gdm)
 */
public class ProctonPecManager extends HttpServlet {
    
private static final Logger log = LogManager.getLogger(ProctonPecManager.class);

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
    //BasicConfigurator.configure();
        
    log.info("--------------------------------");
    log.info("Avvio servlet: " + getClass().getSimpleName());
    log.info("--------------------------------");
    Connection dbConn = null;
    PreparedStatement ps = null;

    int res = -1;
    String idapplicazione = null;
    String tokenapplicazione = null;
    String guidoggetto = null;
    String stato = null;
    String idattivitastepon = null;
    String tipooggetto = null;
        try {
            // leggo i parametri dalla richiesta HTTP

            // dati per l'autenticazione
            idapplicazione = request.getParameter("idapplicazione");
            tokenapplicazione = request.getParameter("tokenapplicazione");

            // dati per l'esecuzione della query
            guidoggetto = request.getParameter("guidoggetto");
            stato = request.getParameter("stato");
            idattivitastepon = request.getParameter("idattivitastepon");
            tipooggetto = request.getParameter("tipooggetto");

            log.debug("dati ricevuti:");
            log.debug("idapplicazione: " + idapplicazione);
            log.debug("guidoggetto: " + guidoggetto);
            log.debug("stato: " + stato);
            log.debug("idattivitastepon: " + idattivitastepon);
            log.debug("tipooggetto: " + tipooggetto);
            
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
            String spedizioniPecTableName = ApplicationParams.getSpedizioniPecTableName();

            if(spedizioniPecTableName == null || spedizioniPecTableName.equals("")) {
                String message = "Manca il nome della tabella nella quale inserire la spedizione PEC da gestire. Indicarlo nel file \"web.xml\"";
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
            
            if(guidoggetto == null || guidoggetto.equals("")) {
                String message = "Manca il parametro \"guidoggetto\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            else if(stato == null || stato.equals("")) {
                String message = "Manca il parametro \"stato\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            else if(idattivitastepon == null || idattivitastepon.equals("")) {
                String message = "Manca il parametro \"idattivitastepon\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            else if(tipooggetto == null || tipooggetto.equals("")) {
                String message = "Manca il parametro \"tipooggetto\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            

            // compongo la query
            String sqlText = "INSERT INTO " + spedizioniPecTableName + 
                      " (guid_oggetto, stato, id_attivita_stepon, tipo_oggetto, id_applicazione)" + 
                      " VALUES(?, ?, ?, ?, ?)";
            ps = dbConn.prepareStatement(sqlText);
            ps.setString(1, guidoggetto);
            ps.setString(2, stato);
            ps.setString(3, idattivitastepon);
            ps.setString(4, tipooggetto);
            ps.setString(5, idapplicazione);
            try {
                String query = ps.toString();
                log.debug("eseguo la query: " + query + " ...");
                res = ps.executeUpdate();
//                if (res == 0)
//                    throw new SQLException("Nessuna riga aggiunta");
            }
            catch (SQLException sQLException) {
                //dbConn.rollback();
                String message = "Errore nell'esecuzione della query\n";
                log.fatal(message, sQLException);
                log.info("dati ricevuti:");
                log.info("idapplicazione: " + idapplicazione);
                log.info("guidoggetto: " + guidoggetto);
                log.info("stato: " + stato);
                log.info("idattivitastepon: " + idattivitastepon);
                log.info("tipooggetto: " + tipooggetto);
                throw new ServletException(message);
            }
            finally {
                if (ps != null)
                    ps.close();
                if (dbConn != null)
                    dbConn.close();
            }
            log.info("operazione completata");
            if (res > 0)
                log.info("aggiunte " + res + " righe");
            else
                log.warn("nessuna riga aggiunta");
            //response.setContentType("text/html;charset=UTF-8");
            PrintWriter out = response.getWriter();
            try {
                out.print(res);
            }
            finally {            
                out.close();
            }
        }
        catch (Exception ex) {
            log.fatal("Errore", ex);
            log.info("dati ricevuti:");
            log.info("idapplicazione: " + idapplicazione);
            log.info("guidoggetto: " + guidoggetto);
            log.info("stato: " + stato);
            log.info("idattivitastepon: " + idattivitastepon);
            log.info("tipooggetto: " + tipooggetto);
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
