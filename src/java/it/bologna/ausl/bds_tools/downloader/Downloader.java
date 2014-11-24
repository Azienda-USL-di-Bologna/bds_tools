package it.bologna.ausl.bds_tools.downloader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.URLEncoder;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
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
    * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
    * parametri:token=(key del token da recuperare da redis)&deletetoken=true|false
    */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    DownloaderPlugin downloaderPluginInstance = null;
        String token = request.getParameter("token");
        String deleteTokenParams = request.getParameter("deletetoken");
        boolean deleteToken = true;
        if (deleteTokenParams != null && deleteTokenParams.toLowerCase().equals("false"))
            deleteToken = false;
        Jedis redis = new Jedis(getServletContext().getInitParameter("redis.host"));
        String params = redis.get(token);
        if (deleteToken)
            redis.del(token);
        redis.disconnect();
        String file = null;
        if (params == null) {
            throw new ServletException("Redis key not found");
        }
        try {
            //"Mongo:mongogdml:504db053b948897cc3354b25"
            //"Redis:redisgdml:filename:key"
            String[] parts = params.split(":", 3);
            String plugin = parts[0];
            String server = parts[1];
            String connParameters = getServletContext().getInitParameter(server);
            if (connParameters==null) {
                throw new ServletException(server + " not found in parameters");
            }
            Class<DownloaderPlugin> pluginClass=(Class<DownloaderPlugin>) Class.forName("it.bologna.ausl.bds_tools.downloader." + plugin + "Downloader", true, this.getClass().getClassLoader());
            Constructor<DownloaderPlugin> downloaderPluginConstructor = pluginClass.getDeclaredConstructor(String.class);
            downloaderPluginInstance = downloaderPluginConstructor.newInstance(connParameters);
            file = parts[2];
        }
        catch (Exception e) {
            throw new ServletException(e);
        } 
        InputStream in=downloaderPluginInstance.getFile(file);
        String fileName=downloaderPluginInstance.getFileName(file);
        OutputStream out=response.getOutputStream();
        response.addHeader("Content-disposition", "attachment;filename=" + "\"" + fileName + "\"");
        response.addHeader("Content-Type", "application/octet-stream");

        byte[] buff=new byte[4096];
        int n=in.read(buff);
        while (n>0) {
            out.write(buff,0,n);
            n=in.read(buff);
        }	
        out.flush();
        IOUtils.closeQuietly(in);
    }

    /**
    * @see HttpServlet#doPo
     * @param requestst(HttpServletRequest request, HttpServletResponse response)
    */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO Auto-generated method stub
        doGet(request,response);
    }
}
