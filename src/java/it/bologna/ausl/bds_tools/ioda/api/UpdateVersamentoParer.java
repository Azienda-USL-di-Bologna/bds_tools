package it.bologna.ausl.bds_tools.ioda.api;

import it.bologna.ausl.bds_tools.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaParerUtilities;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.DatiParer;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.ioda.iodaobjectlibrary.IodaRequest;
import it.bologna.ausl.ioda.iodaoblectlibrary.exceptions.IodaDocumentException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class UpdateVersamentoParer extends HttpServlet{
    
    private static final Logger log = LogManager.getLogger(UpdateVersamentoParer.class);
    
    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.setCharacterEncoding("utf-8");
    // configuro il logger per la console
    //    BasicConfigurator.configure();
    log.info("--------------------------------");
    log.info("Avvio servlet: " + getClass().getSimpleName());
    log.info("--------------------------------");
    IodaParerUtilities iodaParerUtilities;
    Connection dbConn = null;
        try { 
            // leggo i parametri dalla richiesta
            ServletInputStream requestIs = null;
            IodaRequest iodaReq = null;
            try {
                requestIs = request.getInputStream();
                if (requestIs == null) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "json della richiesta mancante");
                    return;
                }
                iodaReq = IodaRequest.parse(request.getInputStream());
            }
            catch (Exception ex) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "formato json della richiesta errato: " + ex.getMessage());
            }
            finally {
                IOUtils.closeQuietly(requestIs);
            }

            // dati per l'autenticazione
            String idapplicazione = iodaReq.getIdApplicazione();
            if (idapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"idApplicazione\" mancante");
                return;
            }
            String tokenapplicazione = iodaReq.getTokenApplicazione();
            if (tokenapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"tokenApplicazione\" mancante");
                return;
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
                prefix = UtilityFunctions.checkAuthentication(dbConn, ApplicationParams.getAuthenticationTable(), idapplicazione, tokenapplicazione);
            }
            catch (NotAuthorizedException ex) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                return;
            }

            DatiParer datiParer;
            try {
                // per convenzione questa servlet userà il tipo di operazione insert
                datiParer = iodaReq.getDatiParer(Document.DocumentOperationType.UPDATE);
            }
            catch (IodaDocumentException ex) {
                log.error(ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }
            
            iodaParerUtilities = new IodaParerUtilities(getServletContext(), datiParer, prefix);

            iodaParerUtilities.updateAggiornamentoParer(dbConn);
        }
        catch (Exception ex) {
            throw new ServletException(ex);
        }
        finally{
            try {
                    dbConn.close();
                }
                catch (Exception ex) {
                }
        }
        
        response.setContentType("text/html;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet AddVersamentoParer</title>");            
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet AddVersamentoParer at " + request.getContextPath() + "</h1>");
            out.println("</body>");
            out.println("</html>");
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
