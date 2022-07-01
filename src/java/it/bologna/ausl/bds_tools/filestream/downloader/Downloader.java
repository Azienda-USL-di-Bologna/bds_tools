package it.bologna.ausl.bds_tools.filestream.downloader;

import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.logging.Level;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

/**
 * Servlet implementation class Downloader
 */
public class Downloader extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LogManager.getLogger(Downloader.class);

    /**
     * @see HttpServlet#HttpServlet()
     */
    public Downloader() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param request
     * @param response
     * @throws javax.servlet.ServletException
     * @throws java.io.IOException
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
     * response) parametri:token=(key del token da recuperare da
     * redis)&deletetoken=true|false
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        log.info("--------------------------------");
        log.info("Avvio servlet: " + getClass().getSimpleName());
        log.info("--------------------------------");

        DownloaderPlugin downloaderPluginInstance = null;
        String token = request.getParameter("token");
        String deleteTokenParams = request.getParameter("deletetoken");
        JSONObject downloadParams = null;
        log.debug("token: " + token);
        log.debug("deleteTokenParams: " + deleteTokenParams);
        boolean deleteToken = true;
        if (deleteTokenParams != null && deleteTokenParams.toLowerCase().equals("false")) {
            deleteToken = false;
        }
        Jedis redis = new Jedis(ApplicationParams.getRedisHost());
        String params = redis.get(token);
        log.debug("params: " + params);
        if (deleteToken) {
            log.debug("deleting token...");
            Long deleted = redis.del(token);
            log.debug("deleted: " + deleted);
        }
        redis.disconnect();
        JSONObject pluginParams = null;
        if (params == null) {
            log.error(String.format("Redis key: %s not found", token));
            throw new ServletException("Redis key not found");
        }
        try {
            //"Mongo:mongogdml:504db053b948897cc3354b25"
            //"Redis:redisgdml:filename:key"
            //{
            //    plugin:
            //    server:
            //    content_type:
            //    force_download:
            //    params:{file_id:,filename:}
            //    }
            //"Mongo{contentType:"p}:mongogdml:504db053b948897cc3354b25"
            downloadParams = (JSONObject) JSONValue.parse(params);
            //String[] parts = params.split(":", 3);
            //String plugin = parts[0];
            String plugin = (String) downloadParams.get("plugin");
            //String server = parts[1];
            String server = (String) downloadParams.get("server");
            String connParameters = null;

            // temporaneo, per farlo funzionare anche prima della fine della modifica delle applicazioni con la nuova modalità
            if (server.contains("gdml") || server.contains("arena") || server.contains("prod") || server.contains("prototipo")) {
                connParameters = getServletContext().getInitParameter(server);
                if (connParameters == null) {
                    throw new ServletException(server + " not found in parameters");
                }
            } else {
                // il parametro server è il nome del parametro da leggere dai parametri pubblici
                connParameters = ApplicationParams.getOtherPublicParam(server);
            }
            Class<DownloaderPlugin> pluginClass = (Class<DownloaderPlugin>) Class.forName("it.bologna.ausl.bds_tools.filestream.downloader." + plugin + "Downloader", true, this.getClass().getClassLoader());
            Constructor<DownloaderPlugin> downloaderPluginConstructor = pluginClass.getDeclaredConstructor(String.class);
            downloaderPluginInstance = downloaderPluginConstructor.newInstance(connParameters);
            pluginParams = (JSONObject) downloadParams.get("params");
        } catch (Exception e) {
            log.error("errore", e);
            throw new ServletException("errore", e);
        }
        InputStream in;
        try {
            in = downloaderPluginInstance.getFile(pluginParams);
        } catch (FileStreamException ex) {
            log.error("errore", ex);
            throw new ServletException("errore", ex);
        }
        String fileName;
        try {
            fileName = downloaderPluginInstance.getFileName(pluginParams);
        } catch (FileStreamException ex) {
            log.error("errore", ex);
            throw new ServletException("errore", ex);
        }
        OutputStream out = response.getOutputStream();

        if (downloadParams.get("content_type") != null && !downloadParams.get("content_type").equals("")) {
            response.addHeader("Content-Type", (String) downloadParams.get("content_type"));
        } else {
            response.addHeader("Content-Type", "application/octet-stream");
        }
        if (downloadParams.get("force_download") != null && (Boolean) downloadParams.get("force_download") == false) {
            response.addHeader("Content-disposition", "inline;filename=" + "\"" + fileName + "\"");
        } else {
            response.addHeader("Content-disposition", "attachment;filename=" + "\"" + fileName + "\"");
        }

        byte[] buff = new byte[4096];
        int n = in.read(buff);
        while (n > 0) {
            out.write(buff, 0, n);
            n = in.read(buff);
        }
        out.flush();
        IOUtils.closeQuietly(in);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO Auto-generated method stub
        doGet(request, response);
    }
}
