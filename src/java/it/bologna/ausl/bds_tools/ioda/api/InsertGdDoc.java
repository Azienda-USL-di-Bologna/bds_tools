package it.bologna.ausl.bds_tools.ioda.api;

import it.bologna.ausl.bds_tools.ApplicationParams;
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
import it.bologna.ausl.bds_tools.ioda.utils.IodaUtilities;
import it.bologna.ausl.iodaobjectlibrary.Document;
import it.bologna.ausl.iodaoblectlibrary.exceptions.IodaDocumentException;
import it.bologna.ausl.iodaoblectlibrary.exceptions.IodaFileException;
import javax.servlet.annotation.MultipartConfig;
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
public class InsertGdDoc extends HttpServlet {
 
private static final Logger log = LogManager.getLogger(InsertGdDoc.class);

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
    IodaUtilities iodaUtilities;
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

            // autenticazione
            if (!UtilityFunctions.checkAuthentication(dbConn, ApplicationParams.getAuthenticationTable(), idapplicazione, tokenapplicazione)) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                try {
                    dbConn.close();
                }
                catch (Exception ex) {
                }
                return;
            }
            
            try {
                iodaUtilities = new IodaUtilities(getServletContext(), request, Document.DocumentOperationType.INSERT, idapplicazione);
            }
            catch (IodaDocumentException ex) {
                log.error(ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }

            try {
                dbConn.setAutoCommit(false);
                
                // inserimento GdDoc con fascicolazione e sottodocumenti
                iodaUtilities.insertGdDoc(dbConn, ps);
                
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
        PrintWriter out = response.getWriter();
        try {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet InsertGdDoc</title>");            
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet InsertGdDoc at " + request.getContextPath() + "</h1>");
            out.println("</body>");
            out.println("</html>");
        } finally {
            out.close();
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
        return "Inserisce un Documento in GEDI";
    }// </editor-fold>

}
