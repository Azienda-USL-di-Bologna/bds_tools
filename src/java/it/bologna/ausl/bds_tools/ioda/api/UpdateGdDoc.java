package it.bologna.ausl.bds_tools.ioda.api;

import it.bologna.ausl.bds_tools.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.ioda.iodaoblectlibrary.exceptions.IodaDocumentException;
import it.bologna.ausl.ioda.iodaoblectlibrary.exceptions.IodaFileException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author gdm
 */
@MultipartConfig(
            fileSizeThreshold   = 1024 * 1024 * 10,  // 10 MB
//            maxFileSize         = 1024 * 1024 * 10, // 10 MB
//            maxRequestSize      = 1024 * 1024 * 15, // 15 MB
            location            = ""
)
public class UpdateGdDoc extends HttpServlet {
private static final Logger log = LogManager.getLogger(UpdateGdDoc.class);

    
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
//    BasicConfigurator.configure();
    log.info("--------------------------------");
    log.info("Avvio servlet: " + getClass().getSimpleName());
    log.info("--------------------------------");
    IodaDocumentUtilities iodaUtilities;
    Connection dbConn = null;
    PreparedStatement ps = null;
        try { 
            // leggo i parametri dalla richiesta
            
            // dati per l'autenticazione
            String idapplicazione = UtilityFunctions.getMultipartStringParam(request, "idapplicazione");
            if (idapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "part \"idapplicazione\" mancante");
                return;
            }
            String tokenapplicazione = UtilityFunctions.getMultipartStringParam(request, "tokenapplicazione");
            if (tokenapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "part \"tokenapplicazione\" mancante");
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
                try {
                    dbConn.close();
                }
                catch (Exception subEx) {
                }
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                return;
            }

            try {
                iodaUtilities = new IodaDocumentUtilities(getServletContext(), request, Document.DocumentOperationType.UPDATE, prefix);
            }
            catch (IodaDocumentException ex) {
                log.error(ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }

            try {
                dbConn.setAutoCommit(false);

                // inserimento GdDoc con fascicolazione e sottodocumenti
                iodaUtilities.updateGdDoc(dbConn, ps);

                dbConn.commit();
            }
            catch (IodaFileException ex) {
                log.error(ex);
                dbConn.rollback();
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }
            catch (Exception ex) {
                log.error(ex);
                dbConn.rollback();
                iodaUtilities.deleteAllMongoFileUploaded();
                throw ex;
            }
            finally {
                if (ps != null)
                    ps.close();
                dbConn.close();
            }
        }
        catch (Exception ex) {
            throw new ServletException(ex);
        }
        
        
        response.setContentType("text/html;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet UpdateGdDoc</title>");            
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet UpdateGdDoc at " + request.getContextPath() + "</h1>");
            out.println("</body>");
            out.println("</html>");
        }
        
         try {
            log.info("converting pdf...");
            iodaUtilities.convertPdf();
        }
        catch (Exception ex) {
            log.error(ex);
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