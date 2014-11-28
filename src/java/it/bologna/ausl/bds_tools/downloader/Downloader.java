package it.bologna.ausl.bds_tools.downloader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

/**
 * Servlet implementation class Downloader
 */
public class Downloader extends HttpServlet {

    private static final long serialVersionUID = 1L;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public Downloader() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
     * response) parametri:token=(key del token da recuperare da
     * redis)&deletetoken=true|false
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        DownloaderPlugin downloaderPluginInstance = null;
        String token = request.getParameter("token");
        String deleteTokenParams = request.getParameter("deletetoken");
        JSONObject downloadParams = null;
        boolean deleteToken = true;
        if (deleteTokenParams != null && deleteTokenParams.toLowerCase().equals("false")) {
            deleteToken = false;
        }
        Jedis redis = new Jedis(getServletContext().getInitParameter("redis.host"));
        String params = redis.get(token);
        if (deleteToken) {
            redis.del(token);
        }
        redis.disconnect();
        JSONObject pluginParams = null;
        if (params == null) {
            throw new ServletException("Redis key not found");
        }
        try {
            //"Mongo:mongogdml:504db053b948897cc3354b25"
            //"Redis:redisgdml:filename:key"
            //{
            //    plugin:
            //    server:
            //    content_type:
            //    download:
            //    params:{file_id:,filename:}
            //    }
            //"Mongo{contentType:"p}:mongogdml:504db053b948897cc3354b25"
            downloadParams = (JSONObject) JSONValue.parse(params);
            //String[] parts = params.split(":", 3);
            //String plugin = parts[0];
            String plugin = (String) downloadParams.get("plugin");
            //String server = parts[1];
            String server = (String) downloadParams.get("server");
            String connParameters = getServletContext().getInitParameter(server);
            if (connParameters == null) {
                throw new ServletException(server + " not found in parameters");
            }
            Class<DownloaderPlugin> pluginClass = (Class<DownloaderPlugin>) Class.forName("it.bologna.ausl.bds_tools.downloader." + plugin + "Downloader", true, this.getClass().getClassLoader());
            Constructor<DownloaderPlugin> downloaderPluginConstructor = pluginClass.getDeclaredConstructor(String.class);
            downloaderPluginInstance = downloaderPluginConstructor.newInstance(connParameters);
            pluginParams = (JSONObject) downloadParams.get("params");
        } catch (Exception e) {
            throw new ServletException(e);
        }
        InputStream in = downloaderPluginInstance.getFile(pluginParams);
        String fileName = downloaderPluginInstance.getFileName(pluginParams);
        OutputStream out = response.getOutputStream();
        //response.addHeader("Content-disposition", "attachment;filename=" + "\"" + fileName + "\"");
        if (downloadParams.get("content_type") != null) {
            response.addHeader("Content-Type", (String) downloadParams.get("content_type"));
        } else {
            response.addHeader("Content-Type", "application/octet-stream");
        }
//response.addHeader("Content-Type", "application/pdf");

        byte[] buff = new byte[4096];
        int n = in.read(buff);
        while (n > 0) {
            out.write(buff, 0, n);
            n = in.read(buff);
        }
        out.flush();
        IOUtils.closeQuietly(in);
    }

    /**
     * @see HttpServlet#doPost
     * @param request(HttpServletRequest request, HttpServletResponse response)
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO Auto-generated method stub
        doGet(request, response);
    }
}
