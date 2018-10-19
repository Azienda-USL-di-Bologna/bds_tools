/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.ioda.api;

import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.ioda.utils.IodaFascicoliUtilities;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicoli;
import it.bologna.ausl.ioda.iodaobjectlibrary.IodaRequestDescriptor;
import it.bologna.ausl.ioda.iodaobjectlibrary.Researcher;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
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
public class HasUserAnyPermissionOnFascicolo extends HttpServlet {
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
        
        log.info("--------------------------------");
        log.info("Avvio servlet: " + getClass().getSimpleName());
        log.info("--------------------------------");
        
        IodaRequestDescriptor iodaReq;
        Connection dbConn = null;
        PreparedStatement ps = null;
        boolean hasPermission;
        
        System.out.println("REQUEST");
        System.out.println(request);
        
        
        IodaFascicoliUtilities fascicoliUtilities;
        
        try {

            try (InputStream is = request.getInputStream()) {
                if (is == null) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "json della richiesta mancante");
                    return;
                }
                iodaReq = IodaRequestDescriptor.parse(is);
            }
            catch (Exception ex) {
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

            try{
                Researcher researcher = (Researcher) iodaReq.getObject();
                
                fascicoliUtilities = new IodaFascicoliUtilities(request, researcher);
                HashMap additionalData = (HashMap) iodaReq.getAdditionalData();

                // la mappa deve essere presa in realtà con iodaReq.getAdditionalData();
                hasPermission = fascicoliUtilities.doesUserHaveAnyPermissionOnThisFascicolo(dbConn, additionalData);

            }
//            catch(SendHttpMessageException | IodaDocumentException | SQLException ex){
//                log.error(ex);
//            }
            finally{
                if (ps != null)
                    ps.close();
                dbConn.close();
            }
            
           
        } catch (Exception ex) {
            throw new ServletException(ex);
        }
        
        response.setContentType("text/html;charset=UTF-8");
        response.addHeader("hasPermssion", hasPermission ? "true" : "false");
        try (PrintWriter out = response.getWriter()) {
            out.print(hasPermission);
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