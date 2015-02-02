package it.bologna.ausl.bds_tools;

import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


/**
 *
 * @author Giuseppe De Marco (gdm)
 */
public class ReceiveFile extends HttpServlet {
private static final Logger log = Logger.getLogger(ReceiveFile.class);
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

        FileItem fileItemTemp = null;
        InputStream fis = null;
        PrintWriter responeWriter = null;
        
        try {
            File tempDir = new File(System.getProperty("java.io.tmpdir"));
            if (!tempDir.exists())
                tempDir.mkdir();
            
            if (ServletFileUpload.isMultipartContent(request)) {
                ServletFileUpload sfu = new ServletFileUpload(new DiskFileItemFactory(1024 * 1024, tempDir));
                List<FileItem> fileItems = sfu.parseRequest(request);
                
                JSONObject params = null;
                for (FileItem fileItem : fileItems) {
                    FileItem item = (FileItem) fileItem;
                    if (item.isFormField()) {
                        log.info("letto: " + item.getString());
                        if (item.getFieldName().equalsIgnoreCase("params")) {
                            if (item.getString() == null || item.getString().equals("")) {
                                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Manca il parametro \"params\"");
                                return;
                            }
                            else
                                params = (JSONObject) JSONValue.parse(URLDecoder.decode((String) item.getString(), "UTF-8"));
                        }
                    }
                    else if (!item.isFormField()) { // Il file ricevuto. Lo salvo su disco
                        System.out.println("Ricevuto il file " + item.getName() + "...");
                        fileItemTemp = item;
                    }
                }

                String uuid;
                try {
                    responeWriter = response.getWriter();
                    fis = fileItemTemp.getInputStream();
                    String serverIdentifier = (String) params.get("serverIdentifier");
                    if (serverIdentifier == null || serverIdentifier.equals("")) {
                        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"serverIdentifier\" non trovato");
                        return;
                    }
                    String mongoDonwnloadParamsName = "mongodownload[server]".replace("[server]", serverIdentifier);
                    String mongoDownloadUrl = getServletContext().getInitParameter(mongoDonwnloadParamsName);

                    if (mongoDownloadUrl == null || mongoDownloadUrl.equals("")) {
                        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "\"mongoDownloadUrl\" non trovato");
                        return;
                    }
                    
                    MongoWrapper m = new MongoWrapper(mongoDownloadUrl);
                    uuid = m.put(fis, fileItemTemp.getName(), "/tmp", false);
                    responeWriter.print(uuid);
                }
                catch (Exception ex) {
                    throw new ServletException(ex);
                }
            }
            else {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "La richiesta non è multipart");
            }
        }
        catch (ServletException ex) {
            throw ex;
        }
        catch (Exception ex) {
            throw new ServletException(ex);
        }
        finally {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(responeWriter);
            if (fileItemTemp != null)
                fileItemTemp.delete();
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
