/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.IodaRequestDescriptor;
import java.awt.GraphicsEnvironment;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
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
 * @author Administrator
 */
public class Debug extends HttpServlet {
private static final Logger log = LogManager.getLogger(Debug.class);




    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

            ServletInputStream requestIs = null;
            IodaRequestDescriptor iodaReq = null;
            try {
                requestIs = request.getInputStream();
                if (requestIs == null) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "json della richiesta mancante");
                    return;
                }
                iodaReq = IodaRequestDescriptor.parse(request.getInputStream());
                System.out.println(iodaReq.getJSONString());
            }
            catch (Exception ex) {
                log.error("formato json della richiesta errato: " + ex);
                ex.printStackTrace();
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "formato json della richiesta errato: " + ex.getMessage());
                return;
            }
            finally {
                IOUtils.closeQuietly(requestIs);
            }
        
        if(true)
            return;
        
        Enumeration<String> parametersNames = request.getParameterNames();
        log.info("parameters:");
        String parameters = "";
        while (parametersNames.hasMoreElements()) {
            String paramName = parametersNames.nextElement();
            parameters += paramName + "=" + request.getParameter(paramName) + "\n";
        }
        log.info(parameters);
        
        log.info("body:");
        String body = UtilityFunctions.inputStreamToString(request.getInputStream());
        log.info(body);
        
        String fonts[] = GraphicsEnvironment.getLocalGraphicsEnvironment().getAvailableFontFamilyNames();
        String output = "";
        for (String font : fonts) {
            output += "\n" + font;
        }


        

        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        try {
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet Debug</title>");            
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet Debug at " + request.getContextPath() + "</h1>");
            out.println("<p>parameters: " + parameters + "</p>");
            out.println("<p>body: " + body + "</p>");
//            out.println("<p> fonts: trovati: " + fonts.length + "</p>");
//            out.println("<p> lista: <br/>" + output.replace("\n", "<br/>\n") + "</p>");
            out.println("</body>");
            out.println("</html>");
        } finally {
            out.close();
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
