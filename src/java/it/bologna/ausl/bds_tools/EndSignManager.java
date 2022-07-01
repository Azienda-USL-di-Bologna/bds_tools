package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.redis.redispubsub.Publisher;
import java.io.IOException;
import java.io.InputStreamReader;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author Davide Bacca
 */
public class EndSignManager extends HttpServlet {
private static final String RESULT_CHANNEL = "ResultChannel";
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
        //qui prendiamo il risultato e lo parsiamo in Json
        JSONObject result = (JSONObject) JSONValue.parse(new InputStreamReader(request.getInputStream()));
        
        //prendo dal json il nome del canale redis in cui inserire (ricevuto in input dall'applet, che a sua volta ha ricevuto in input dall'applicazione)
        String resultChannel = (String) result.get(RESULT_CHANNEL);
        if ((resultChannel == null || resultChannel.isEmpty()) && result.containsKey("endSignParams")) {
            JSONObject endSignParams = (JSONObject)result.get("endSignParams");
            resultChannel = (String) endSignParams.get(RESULT_CHANNEL);
        }

        // istanzio la classe Pubblisher di redis_client per pubblicare il risultato che sarà letto dall'applicazione
        Publisher redisPublisher = new Publisher(ApplicationParams.getRedisHost(), -1);
        
        // pubblico il risultato sul canale
        redisPublisher.publish(resultChannel, result.toJSONString());
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
