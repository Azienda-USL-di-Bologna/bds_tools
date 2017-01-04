package it.bologna.ausl.bds_tools.ioda.api;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.exceptions.RequestException;
import it.bologna.ausl.bds_tools.exceptions.ResourceNotAvailableException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.ioda.utils.IodaUtilities;
import it.bologna.ausl.bds_tools.ioda.utils.LockUtilities;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.IodaRequestDescriptor;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaFileException;
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
            
            IodaRequestDescriptor iodaRequest;
            try {
                iodaRequest = IodaUtilities.extractIodaRequestFromMultipartRequest(request);
            }
            catch (RequestException ex) {
                log.error("errore nell'estrazione della richiesta: ", ex);
                response.sendError(ex.getHttpStatusCode(), ex.getMessage());
                return;
            }
            
            // dati per l'autenticazione
            String idapplicazione = iodaRequest.getIdApplicazione();
            String tokenapplicazione = iodaRequest.getTokenApplicazione();
            
            // ottengo una connessione al db
            try {
                dbConn = UtilityFunctions.getDBConnection();
            }
            catch (SQLException sQLException) {
                String message = "Problemi nella connesione al Data Base. Indicare i parametri corretti nei file di configurazione dell'applicazione" + "\n" + sQLException.getMessage();    
                log.error(message, sQLException);
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

            try {
                //iodaUtilities = new IodaDocumentUtilities(getServletContext(), request, Document.DocumentOperationType.UPDATE, prefix);
                iodaUtilities = new IodaDocumentUtilities(iodaRequest, Document.DocumentOperationType.UPDATE, prefix);
            }
            catch (IodaDocumentException ex) {
                try {
                    dbConn.close();
                }
                catch (Exception subEx) {
                }
                log.error("errore nella gestione del gdddoc: ", ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }

            LockUtilities lock = null;
            try {
                dbConn.setAutoCommit(false);

                GdDoc gdDoc = iodaUtilities.getGdDoc();
                lock = new LockUtilities(gdDoc.getIdOggettoOrigine(), gdDoc.getTipoOggettoOrigine(), String.valueOf(System.currentTimeMillis()), ApplicationParams.getServerId(), ApplicationParams.getRedisHost());
                
                int times = 0;
                boolean updateComplete = false;
                while (!updateComplete && times < ApplicationParams.getResourceLockedMaxRetryTimes()) {
                    if (lock.getLock()) {

                        // inserimento GdDoc con fascicolazione e sottodocumenti
                        iodaUtilities.updateGdDoc(dbConn, iodaRequest.getAdditionalData());
                        String numero, numeroAndAnno = null;
                        if(gdDoc.getNumerazioneAutomatica() != null && gdDoc.getNumerazioneAutomatica()){
                            if (gdDoc.getCodiceRegistro() == null || gdDoc.getCodiceRegistro().equals("")) {
                                gdDoc.setCodiceRegistro(ApplicationParams.getDefaultSequenceName());
                            }
                            numeroAndAnno = iodaUtilities.registraDocumento(dbConn, ps, gdDoc.getGuid(), gdDoc.getCodiceRegistro());
                        }
                        
                        numero = numeroAndAnno;
                        // TODO:da sistemare
                        if (numero != null && numeroAndAnno.contains("/")) {
                            numero = numeroAndAnno.split("/")[0];
                        }
                        
                        dbConn.commit();
                        if(gdDoc.getNumerazioneAutomatica() != null && gdDoc.getNumerazioneAutomatica())
                            log.info("numero assegnato: " + numero);
                        updateComplete = true;
                    }
                    Thread.sleep(ApplicationParams.getResourceLockedSleepMillis());
                    times++;
                }
                if (!updateComplete) {
                    //dare eccezione per risorsa non disponibile
                    String message = "Risorsa non disponibile perchè occupata da un altro processo. Riprovare più tardi";
                    throw new ResourceNotAvailableException(message);
                }
            }
            catch (IodaFileException ex) {
                log.error("errore nella gestione del gdddoc: ", ex);
                dbConn.rollback();
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }
            catch (ResourceNotAvailableException ex) {
                // eccezione che da risorsa non disponibile
                log.error("errore nella gestione del gdddoc: ", ex);
                response.sendError(HttpServletResponse.SC_CONFLICT, ex.getMessage());
                return;
            }
            catch (Exception ex) {
                log.error("errore nella gestione del gdddoc: ", ex);
                dbConn.rollback();
                iodaUtilities.deleteAllMongoFileUploaded();
                throw ex;
            }
            finally {
                if (ps != null)
                    ps.close();
                dbConn.close();
                if (lock != null)
                    lock.deleteLock(false);
            }
        }
        catch (Exception ex) {
            log.error("errore della servlet: ", ex);
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
            log.error("errore nella conversione in pdf: ", ex);
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
        return "Update GdDoc";
    }// </editor-fold>

}
