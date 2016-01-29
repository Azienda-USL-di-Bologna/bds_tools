package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URLDecoder;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author Giuseppe De Marco (gdm)
 */
@MultipartConfig(
        fileSizeThreshold = 1024 * 1024 * 10, // 10 MB
        //            maxFileSize         = 1024 * 1024 * 10, // 10 MB
        //            maxRequestSize      = 1024 * 1024 * 15, // 15 MB
        location = ""
)
public class ReceiveFile extends HttpServlet {

private static final Logger log = LogManager.getLogger(ReceiveFile.class);

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

        InputStream fis = null;
        String fileName;
        PrintWriter responeWriter = null;

        try {
            JSONObject params;
            try {
                params = (JSONObject) JSONValue.parse(URLDecoder.decode(UtilityFunctions.getMultipartStringParam(request, "params"), "UTF-8"));
            }
            catch (Exception ex) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "errore nel parsing della form part \"params\": " + ex);
                return;
            }

            Part filePart = request.getPart("upload-file");
            log.info("letto: " + filePart.getName());
            fis = filePart.getInputStream();
            fileName = filePart.getSubmittedFileName();
            System.out.println("Ricevuto il file " + fileName + "...");

            String uuid;
            try {
                if (fis == null) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Nessun file trovato nella richiesta");
                    return;
                }

                if (params == null) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Manca il parametro \"params\"");
                    return;
                }
                responeWriter = response.getWriter();
                String serverIdentifier = (String) params.get("serverIdentifier");
                if (serverIdentifier == null || serverIdentifier.equals("")) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"serverIdentifier\" non trovato");
                    return;
                }
                
                String mongoDownloadUrl = ApplicationParams.getMongoDownloadUri();

                if (mongoDownloadUrl == null || mongoDownloadUrl.equals("")) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"mongoDownloadUrl\" non trovato");
                    return;
                }

                MongoWrapper m = new MongoWrapper(mongoDownloadUrl);
                uuid = m.put(fis, fileName, "/tmp", false);
                responeWriter.print(uuid);
            } catch (Exception ex) {
                throw new ServletException(ex);
            }
        } catch (ServletException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ServletException(ex);
        } finally {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(responeWriter);
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
        return "Riceve un file, lo salva su mongodownload e torna l'uuid nel body";
    }// </editor-fold>

}
