/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.ioda.api;

import it.bologna.ausl.bds_tools.ApplicationParams;
import it.bologna.ausl.bds_tools.ioda.Document;
import it.bologna.ausl.bds_tools.ioda.GdDoc;
import it.bologna.ausl.bds_tools.ioda.IodaRequest;
import it.bologna.ausl.bds_tools.ioda.utils.IodaUtilities;
import it.bologna.ausl.bds_tools.ioda.utils.exceptions.IodaDocumentException;
import it.bologna.ausl.bds_tools.ioda.utils.exceptions.IodaFileException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author gdm
 */
public class DeleteGdDoc extends HttpServlet {
private static final Logger log = LogManager.getLogger(DeleteGdDoc.class);
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
            ServletInputStream requestIs = null;
            IodaRequest iodaReq;
            try {
                requestIs = request.getInputStream();
                if (requestIs == null) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "json della richiesta mancante");
                    return;
                }
                iodaReq = IodaRequest.parse(request.getInputStream());
            }
            finally {
                IOUtils.closeQuietly(requestIs);
            }

            // dati per l'autenticazione
            String idapplicazione = iodaReq.getIdApplicazione();
            if (idapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "part \"idapplicazione\" mancante");
                return;
            }
            String tokenapplicazione = iodaReq.getTokenApplicazione();
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

            GdDoc gdDoc;
            try {
                gdDoc = iodaReq.getGdDoc(Document.DocumentOperationType.DELETE);
            }
            catch (IodaDocumentException ex) {
                log.error(ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }
            
            try {
                iodaUtilities = new IodaUtilities(getServletContext(), gdDoc, Document.DocumentOperationType.DELETE, idapplicazione);
            }
            catch (IodaDocumentException ex) {
                log.error(ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }

            try {
                dbConn.setAutoCommit(false);
                iodaUtilities.deleteGdDoc(dbConn, ps);
                dbConn.commit();
            }
            catch (IodaDocumentException ex) {
                log.error(ex);
                dbConn.rollback();
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
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
            out.println("<title>Servlet DeleteGdDoc</title>");            
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet DeleteGdDoc at " + request.getContextPath() + "</h1>");
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
