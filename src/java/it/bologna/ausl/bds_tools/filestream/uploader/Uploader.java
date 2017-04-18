package it.bologna.ausl.bds_tools.filestream.uploader;

import com.google.gson.JsonArray;
import it.bologna.ausl.bds_tools.*;
import it.bologna.ausl.bds_tools.filestream.downloader.DownloaderPlugin;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

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
public class Uploader extends HttpServlet {

    private static final Logger log = LogManager.getLogger(Uploader.class);

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

        PrintWriter responeWriter = null;

//        {
//            "server":"mongoConnectionString",
//            "plugin":"Mongo",
//            "force_download":true,
//            "delete_token":true,
//            "params":{
//                "path":"/tmp/test"
//            }
//        }
        try {
            JSONObject metaData = null;
            List<Part> files = new ArrayList<>();

            for (Part part : request.getParts()) {
                if (part.getName().equalsIgnoreCase("metadata")) {

                    try {
                        metaData = (JSONObject) JSONValue.parse(URLDecoder.decode(UtilityFunctions.getMultipartStringParam(request, "metadata"), "UTF-8"));
                    } catch (Exception ex) {
                        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "errore nel parsing della form part \"metadata\": " + ex);
                        return;
                    }
                } else if (part.getName().equalsIgnoreCase("files")) {
                    files.add(part);
                } else {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, String.format("nome part: %s non valido", part.getName()));
                    return;
                }
            }

            if (metaData == null || metaData.isEmpty()) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "metadata vuoto");
                return;
            }
            if (files == null || files.isEmpty()) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "nessun file trovato nella richiesta");
                return;
            }

            String plugin = (String) metaData.get("plugin");
            String server = (String) metaData.get("server");
            Boolean forceDownload = (Boolean) metaData.get("force_download");
            Boolean deleteToken = (Boolean) metaData.get("delete_token");

            String connParameters = ApplicationParams.getOtherPublicParam(server);

            Class<UploaderPlugin> pluginClass = (Class<UploaderPlugin>) Class.forName("it.bologna.ausl.bds_tools.filestream.uploader." + plugin + "Uploader", true, this.getClass().getClassLoader());
            Constructor<UploaderPlugin> uploaderPluginConstructor = pluginClass.getDeclaredConstructor(String.class);
            UploaderPlugin uploaderPluginInstance = uploaderPluginConstructor.newInstance(connParameters);

            String basePath = (String) metaData.get("path");
            JSONArray jsonArray = new JSONArray();
            Jedis redis = new Jedis(ApplicationParams.getRedisHost());

            for (Part filePart : files) {
                String fileName = filePart.getSubmittedFileName();
                InputStream file = filePart.getInputStream();

                String res = uploaderPluginInstance.putFile(file, basePath, fileName);

                JSONObject downloaderParams = new JSONObject();
                downloaderParams.put("server", server);
                downloaderParams.put("plugin", plugin);
                downloaderParams.put("content_type", filePart.getContentType());
                downloaderParams.put("force_download", forceDownload);

                JSONObject params = new JSONObject();
                params.put("file_name", fileName);
                params.put("file_id", res);

                downloaderParams.put("params", params);
                String token = UUID.randomUUID().toString();

                redis.set(token, downloaderParams.toJSONString());

                // costruirsi link per il download da restituire
                // {"server":"mongoConnectionString","plugin":"Mongo","content_type":"","force_download":true,"params":{"file_name":"","file_id":"58bd11a582cea552acdd56b9"}}
            }

        } catch (Exception ex) {
            throw new ServletException(ex);
        } finally {
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
