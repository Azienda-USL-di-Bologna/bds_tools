package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Andrea
 */
public class GetFascicoloSpecialeId extends HttpServlet {

    private static final Logger log = LogManager.getLogger(GetFascicoloSpecialeId.class);
    public enum TipiFascicoli{ATTI, REGISTRO, DETERMINE, DELIBERE};
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
        
        request.setCharacterEncoding("utf-8");
        // configuro il logger per la console
        //BasicConfigurator.configure();
        
        log.info("--------------------------------");
        log.info("Avvio servlet: " + getClass().getSimpleName());
        log.info("--------------------------------");
        
        String idapplicazione = null;
        String tokenapplicazione;
        Connection dbConn = null;
        PreparedStatement ps;
        String annoString = null;
        int anno = 0;
        //Tipo fascicolo corrisponde al nome del sub-fascicolo speciale: "Registro", "Determine", "Delibere"
        
        String nomeFascicolo = null;
        String id_fascicolo= null;

        try
        {
             // dati per l'autenticazione
            idapplicazione = request.getParameter("idapplicazione");
            tokenapplicazione = request.getParameter("tokenapplicazione");
            
            if (idapplicazione == null || tokenapplicazione == null) {
                String message = "Dati di autenticazione errati, specificare i parametri \"idapplicazione\" e \"tokenapplicazione\" nella richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            
            annoString = request.getParameter("anno");
            nomeFascicolo = request.getParameter("nomefascicolo");
            log.info("Dati Ricevuti: ");
            log.info("anno: " + annoString);
            log.info("nomefascicolo: " + nomeFascicolo);
            anno = Integer.parseInt(annoString);
                                
            
           try {
                dbConn = UtilityFunctions.getDBConnection();
               }
            catch (SQLException sQLException) {
                String message = "Problemi nella connesione al Data Base. Indicare i parametri corretti nei file di configurazione dell'applicazione" + "\n" + sQLException.getMessage();    
                log.error(message);
                throw new ServletException(message);
            }
            
            String fascicoliTable = getServletContext().getInitParameter("FascicoliTableName");
            
            if(fascicoliTable == null || fascicoliTable.equals("")) {
                String message = "Manca il nome della tabella fascicoli. Indicarlo nel file \"web.xml\"";
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
            
            if(annoString == null || annoString.equals("")) {
                String message = "Manca il parametro \"anno\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            else if(nomeFascicolo == null || nomeFascicolo.equals(""))
            {
                String message = "Manca il parametro \"nomefascicolo\". Indicarlo nei parametri della richiesta";
                log.error(message);
                throw new ServletException(message);
            }
                        
            ResultSet result = null;        
            String idFascicoloRootSpeciale = null;    
            
            //Recupero la root dei fascicoli speciali
            String query1 = "SELECT id_fascicolo FROM " + fascicoliTable + " WHERE id_livello_fascicolo = ? AND speciale = ? AND anno_fascicolo = ? " ;
            ps = dbConn.prepareStatement(query1);
            ps.setString(1, "1");
            ps.setInt(2, -1);
            ps.setInt(3, anno);
            result = ps.executeQuery();
            
            if( result != null && result.next() == true)
            {
                idFascicoloRootSpeciale = result.getString(1);
                
                //Compongo la query
                String queryText =  "SELECT f.id_fascicolo as id_fascicolo_figlio " +
                "FROM gd.patriarcato_fascicoli(?) pf " +
                "JOIN gd.fascicoligd f on pf.id_fascicolo=f.id_fascicolo " + 
                "WHERE f.nome_fascicolo = ?";

//                String queryText =  "SELECT fp.id_fascicolo_figlio " +
//                                "FROM gd.fascicoli_patriarcato fp " +
//                                "JOIN gd.fascicoligd f on f.id_fascicolo = fp.id_fascicolo_figlio " +
//                                "WHERE fp.id_fascicolo = ? " +
//                                "AND f.nome_fascicolo = ? ";
            
                ps = dbConn.prepareStatement(queryText);
                ps.setString(1, idFascicoloRootSpeciale);
                ps.setString(2, nomeFascicolo );
                result = ps.executeQuery();        
            
                if( result != null && result.next() == true)
                {
                  id_fascicolo = result.getString(1);
                }
            }
            else
            {
                throw new ServletException("Fascicolo Speciale Root non trovato");
            }
                        
        }
        catch (Exception ex) {
            log.fatal("Errore", ex);
            log.info("dati ricevuti:");
            log.info("idapplicazione: " + idapplicazione);
            log.info("anno: " + annoString);
            log.info("tipoFascicolo: " + nomeFascicolo);
            throw new ServletException(ex);
        }
        finally
        {
            try {
                //Chiudo connessione

                dbConn.close();
            } catch (SQLException ex) {
                throw new ServletException(ex);
            }
        }
        
        
        ServletOutputStream out = response.getOutputStream();
        
       
        try {
            /* TODO output your page here. You may use following sample code. */
            out.print(id_fascicolo);
            System.out.println(id_fascicolo);
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
