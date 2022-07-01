package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.exceptions.SignExeption;
import it.bologna.ausl.bds_tools.exceptions.UtilityFunctionException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.exceptions.MongoWrapperException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.Security;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.json.simple.JSONObject;

/**
 *
 * @author Giuseppe De Marco (gdm)
 */
public class SetDocumentNumber extends HttpServlet {

private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(SetDocumentNumber.class);
    /** 
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    request.setCharacterEncoding("utf-8");
    // configuro il logger per la console
//    BasicConfigurator.configure();
    log.info("--------------------------------");
    log.info("Avvio servlet: " + getClass().getSimpleName());
    log.info("--------------------------------");

    Connection dbConn = null;
    

    String idapplicazione = null;
    String tokenapplicazione = null;
    String iddocumento = null;
    String nomesequenza = null;
    String checkfirmaString = null;
    
    Boolean checkFirma = true;

        try {
            // leggo i parametri dalla richiesta HTTP

            // dati per l'autenticazione
            idapplicazione = request.getParameter("idapplicazione");
            tokenapplicazione = request.getParameter("tokenapplicazione");

            // dati per l'esecuzione della query
            iddocumento = request.getParameter("iddocumento");
            nomesequenza = request.getParameter("nomesequenza");
            checkfirmaString = request.getParameter("checkfirma");

            log.debug("dati ricevuti:");
            log.debug("idapplicazione: " + idapplicazione);
            log.debug("iddocumento: " + iddocumento);
            log.debug("nomesequenza: " + nomesequenza);
            log.debug("checkfirma: " + checkfirmaString);
            
            // controllo se mi sono stati passati i dati per l'autenticazione
            if (idapplicazione == null || tokenapplicazione == null) {
                String message = "Dati di autenticazione errati, specificare i parametri \"idapplicazione\" e \"tokenapplicazione\" nella richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            
            if (iddocumento == null || iddocumento.equals("")) {
                String message = "Dati errati, specificare il parametro \"iddocumento\" nella richiesta";
                log.error(message);
                throw new ServletException(message);
            }
            
            if (nomesequenza == null || nomesequenza.equals("")) {
                String message = "Dati errati, specificare il parametro \"nomesequenza\" nella richiesta";
                log.error(message);
                throw new ServletException(message);
            }

            if (checkfirmaString == null || checkfirmaString.equals("")) {
                checkFirma = true;
                log.info("checkfirma null, quindi implicitamente considerato true (solo nei casi di protocollo in uscita, determina e delibera)");
            } else {
                if (checkfirmaString.equalsIgnoreCase("false")) {
                    checkFirma = false;
                    log.warn("ricevuto parametro che indica di non controllare la firma prima della numerazione");
                }
            }

            // ottengo una connessione al db
            try {
                dbConn = UtilityFunctions.getDBConnection();
            }
            catch (SQLException sQLException) {
                String message = "Problemi nella connesione al Data Base. Indicare i parametri corretti nei file di configurazione dell'applicazione" + "\n" + sQLException.getMessage();    
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

            dbConn.setAutoCommit(false);
            if (checkFirma) {
                // controlla che il documento sia firmato (solo protocollo in uscita, determine e delibere), se non lo è la funzione tornerà eccezione
                SetDocumentNumber.checkFirma(dbConn, iddocumento, nomesequenza);
            }
            String number = SetDocumentNumber.setNumber(dbConn, iddocumento, nomesequenza);
            dbConn.commit();
            
            response.setContentType("text/plain");
            try (PrintWriter out = response.getWriter()) {
                out.print(number);
            }
        }
        catch (Throwable ex) {
            log.fatal("Errore", ex);
            log.info("dati ricevuti:");
            log.info("idapplicazione: " + idapplicazione);
            log.info("iddocumento: " + iddocumento);
            log.info("nomeregistro: " + nomesequenza);
            log.info("checkfirma: " + checkfirmaString);
            try {
                if (dbConn != null && dbConn.getAutoCommit() == false)
                    dbConn.rollback();
//                if (dbConn != null)
//                    dbConn.close();
            }
            catch (SQLException sQLException) {
                log.fatal("Errore nel rollback dell'operazione", sQLException);
            }
            throw new ServletException(ex);
        }
        finally {
            if (dbConn != null)
                try {
                    dbConn.close();
                }
                catch (SQLException ex) {
                    log.error("Errore nella chiusura della connessione", ex);
                }
        }
    }
    
    /**
     * Controlla, solo se la sequenza indica la numerazione di un protocollo, di una determina o di uan delibera, che esista il testo firmato
     * NB: Per i protocolli il controllo viene effettuato solo se il protocollo è in uscita
     * @param dbConn
     * @param idDocumento
     * @param nomesequenza
     * @throws SQLException 
     * @throws it.bologna.ausl.bds_tools.exceptions.SignExeption 
     */
    public static void checkFirma(Connection dbConn, String idDocumento, String nomesequenza) throws SQLException, SignExeption {
        Security.addProvider(new BouncyCastleProvider());
        String sqlText;
        log.info(String.format("numerazione %s in sequenza %s...", idDocumento, nomesequenza));
        switch (nomesequenza) {
            case "protocollo_generale":
                sqlText = "select uuid_testo_firmato, movimentazione from " + ApplicationParams.getDocumentiTableName() + " where guid_documento = ?";
                try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
                    ps.setString(1, idDocumento);

                    String query = ps.toString();
                    log.debug("eseguo la query: " + query + " ...");
                    ResultSet result = ps.executeQuery();
                    boolean nextRow = result.next();
                    if (nextRow == false)
                        throw new SQLException("errore nel controllo del file firmato prima della numerazione, il documento non esiste");
                    else {
                        String uuidFileFirmato = result.getString("uuid_testo_firmato");
                        String movimentazione = result.getString("movimentazione");
                        // l'errore lo deve dare solo se il protocollo è in uscita e il file firmato non c'è.
                        // il protocollo è in uscita solo se la movimentazione è "out" o "io"
                        if ((movimentazione.equalsIgnoreCase("out") || movimentazione.equalsIgnoreCase("io")) && uuidFileFirmato == null) {
                            throw new SQLException("si sta cercando di numerare un documento senza che esista il file firmato");
                        }
                    }
                }
            break;
            case "determine":
                sqlText = "select uuid_testo_firmato_no_omissis from " + ApplicationParams.getDetermineTableName()+ " where guid_determina = ?";
                try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
                    ps.setString(1, idDocumento);

                    String query = ps.toString();
                    log.debug("eseguo la query: " + query + " ...");
                    ResultSet result = ps.executeQuery();
                    boolean nextRow = result.next();
                    if (nextRow == false)
                        throw new SQLException("errore nel controllo del file firmato prima della numerazione, il documento non esiste");
                    else {
                        // il campo "uuid_testo_firmato_no_omissis" deve essere sempre popolato se la determina è firmata correttamente
                        String uuidFileFirmato = result.getString("uuid_testo_firmato_no_omissis");
                        if (uuidFileFirmato == null) {
                            throw new SQLException("si sta cercando di numerare un documento senza che esista il file firmato");
                        }
                    }
                }
            break;
            case "delibere":
                sqlText = "select uuid_testo_firmato_no_omissis from " + ApplicationParams.getDelibereTableName()+ " where guid_delibera = ?";
                try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
                    ps.setString(1, idDocumento);

                    String query = ps.toString();
                    log.info("eseguo la query: " + query + " ...");
                    ResultSet result = ps.executeQuery();
                    boolean nextRow = result.next();
                    if (nextRow == false)
                        throw new SQLException("errore nel controllo del file firmato prima della numerazione, il documento non esiste");
                    else {
                        // il campo "uuid_testo_firmato_no_omissis" deve essere sempre popolato se la delibera è firmata correttamente
                        String uuidFileFirmato = result.getString("uuid_testo_firmato_no_omissis");
                        log.info(String.format("uuid file firmato: %s", uuidFileFirmato));
                        if (uuidFileFirmato == null) {
                            throw new SQLException("si sta cercando di numerare un documento senza che esista il file firmato");
                        } else {
                            // se siamo in ambiente di produzione controllo che sulla delibera ci siano le firme di tutti i firmatari attesi
                            if (ApplicationParams.getAmbienteProduzione()) {
                                log.info("rilevato ambiente di produzione, controllo i firmatari della delibera");
                                checkFirmatariDeli(dbConn, idDocumento, uuidFileFirmato);
                            }
                        }
                    }
                }
            break;
        }
    }
    
    public static void checkFirmatariDeli(Connection dbConn, String idDocumento, String uuidFileFirmato) throws SignExeption {
        File signedFile = null;
        try {
            try {
                signedFile = downloadFile(uuidFileFirmato);
            } catch (Exception ex) {
                String errorMessage = "errore nel download del file firmato da controllare";
                log.error(errorMessage, ex);
                throw new SignExeption(errorMessage, ex);
            }
            Map<String, Object>[] signers;
            try {
                signers = UtilityFunctions.getSigners(signedFile);
            } catch (UtilityFunctionException ex) {
                String errorMessage = "errore nell'estrazione delle firme dalla delibera firmata";
                log.error(errorMessage, ex);
                return;
                //throw new SignExeption(errorMessage, ex);
            }

            String sqlFirmatari = 
                "select u.cf as cf, u.descrizione as descrizione_utente from " +
                "deli.attori_delibere ad inner join " +
                ApplicationParams.getDelibereTableName() + " d on ad.id_delibera = d.id_delibera inner join " +
                "procton.utenti u on ad.id_utente = u.id_utente inner join " +
                "deli.azioni_processo a on ad.id_azione_processo = a.id_azione_processo " +
                "where a.firma <> 0 and " +
                "d.guid_delibera = ?";
                
                // log.debug("query firmatari: " + sqlFirmatari + " ...");

            try (PreparedStatement ps = dbConn.prepareStatement(sqlFirmatari)) {
                ps.setString(1, idDocumento);

                String query = ps.toString();
                log.info("eseguo la query: " + query + " ...");
                ResultSet result = ps.executeQuery();
                int numeroFirmatari = 0;
                while (result.next()) {
                    numeroFirmatari ++;
                    String codiceFiscale = result.getString("cf");
                    String descrizioneUtente = result.getString("descrizione_utente");
                    if (!Arrays.stream(signers).anyMatch(s -> s.get("codiceFiscale").equals(codiceFiscale))) {
                        String errorMessage = String.format("manca la firma di %s", descrizioneUtente);
                        log.error(errorMessage);
                        //throw new SignExeption(errorMessage);
                    }
                }
            } catch (Exception ex) {
                String errorMessage = "errore nell'esecuzione della query per il calcolo dei firmatari";
                log.error(errorMessage, ex);
                // throw new SignExeption(errorMessage, ex);
            }
        } finally {
            if (signedFile != null) {
                signedFile.delete();
            }
        }
    }
    
    public static File downloadFile(String uuid) throws FileStreamException, IOException, MongoWrapperException {
        MongoWrapper m;
        try {
            boolean useMinIO = ApplicationParams.getUseMinIO();
            JSONObject minIOConfig = ApplicationParams.getMinIOConfig();
            String codiceAzienda = ApplicationParams.getCodiceAzienda();
            Integer maxPoolSize = Integer.parseInt(minIOConfig.get("maxPoolSize").toString());
            String mongoUri = ApplicationParams.getMongoRepositoryUri();
            m = MongoWrapper.getWrapper(
                    useMinIO,
                    mongoUri,
                    minIOConfig.get("DBDriver").toString(),
                    minIOConfig.get("DBUrl").toString(),
                    minIOConfig.get("DBUsername").toString(),
                    minIOConfig.get("DBPassword").toString(),
                    codiceAzienda,
                    maxPoolSize,
                    null);
        } catch (Exception ex) {
            throw new FileStreamException(ex);
        }
        
        File signedFile = File.createTempFile("checkFirmaInSetDocumentNumber_", ".tmp");
        signedFile.deleteOnExit();
        try (InputStream is = m.get(uuid);) {
            try (FileOutputStream fos = new FileOutputStream(signedFile);) {
                IOUtils.copy(is, fos);
            }
        }
        return signedFile;
    }
    
    public static String setNumber(Connection dbConn, String idDocumento, String nomesequenza) throws ServletException, SQLException {

        String res = null;
        String updateNumberFunctionName = ApplicationParams.getUpdateNumberFunctionNameTemplate();
        updateNumberFunctionName = updateNumberFunctionName.replace("[nome_sequenza]", nomesequenza);

        if(updateNumberFunctionName == null || updateNumberFunctionName.equals("")) {
            String message = "Errore nel calcolo del nome della funzione che esegue l'aggiornamento del numero di documento";
            log.error(message);
            throw new ServletException(message);
        }

        // compongo la query
        //dbConn.setAutoCommit(false);
        String sqlText = "select * from " + updateNumberFunctionName + "(?, ?)";
        PreparedStatement ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idDocumento);
        ps.setString(2, nomesequenza);
        try {
            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = ps.executeQuery();
            boolean nextRow = result.next();
            if (nextRow == false)
                throw new SQLException("errore nella numerazione");
            else {
                res = result.getString(1);
                
                try {
                    res = res + "/" + String.valueOf(result.getInt(2));
                }
                catch (Exception ex) {
                    log.warn("errore nella lettura dell'anno del protocollo, forse non c'è");
                }
                
                if (res == null || res.equals(""))
                    throw new SQLException("errore nella numerazione");
                //dbConn.commit();
            }
        }
        finally {
            if (ps != null)
                ps.close();
        }
        return res;
    }
    
    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /** 
     * Handles the HTTP <code>GET</code> method.
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
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>
}