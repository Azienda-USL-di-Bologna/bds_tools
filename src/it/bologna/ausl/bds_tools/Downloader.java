package it.bologna.ausl.bds_tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		DownloaderPlugin d=null;
		String token=request.getParameter("token");
		Jedis redis=new Jedis(getServletContext().getInitParameter("redis.host"));
		String params=redis.get(token);
		redis.del(token);
		redis.disconnect();
		String file=null;
		if (params==null){throw new ServletException("Redis key not found");}
		try {
			//"mongogdml:504db053b948897cc3354b25"
			String[] parts=params.split(":",2);
			String server=parts[0];
			String connParameters=getServletContext().getInitParameter(server);
			if (connParameters==null)
			{
				throw new ServletException(server+" not found in parameters");
			}
			d=new MongoDownloader(connParameters);
		    file=parts[1];
		} catch (Exception e) {
			
			throw new ServletException(e);
		} 
		InputStream in=d.getFile(file);
		OutputStream out=response.getOutputStream();
		byte[] buff=new byte[4096];
		while (in.read(buff)>0){
			out.write(buff);
			out.flush();
		}
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request,response);
	}

}
