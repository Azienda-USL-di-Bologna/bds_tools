package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.exceptions.RequestException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.exceptions.MongoWrapperException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.logging.Level;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import org.apache.commons.io.IOUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

/**
 *
 * @author Andrea
 */
@MultipartConfig(
        fileSizeThreshold = 1024 * 1024 * 10, // 10 MB
        //            maxFileSize         = 1024 * 1024 * 10, // 10 MB
        //            maxRequestSize      = 1024 * 1024 * 15, // 15 MB
        location = ""
)
public class UploadGdDocInFascicolo extends HttpServlet {

    private static final Logger log = LogManager.getLogger(UploadGdDocInFascicolo.class);

    private Connection dbConn = null;

    private MongoWrapper mongo;

    private Timestamp currentDate;

    private String idGdDocInserito = null;
    private String fileNameToCreate = null;

    private final String SQL_EXCEPTION_DUPLICATED_ITEM = "23";

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     * @throws it.bologna.ausl.bds_tools.exceptions.RequestException
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException, RequestException {

        request.setCharacterEncoding("utf-8");
//        PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource("it/bologna/ausl/bds_tools/conf/log4j.properties"));
        // configuro il logger per la console
//        BasicConfigurator.configure();

        log.info("--------------------------------");
        log.info("Avvio servlet: " + getClass().getSimpleName());
        log.info("--------------------------------");

        //Dichiarazione variabili
        String idapplicazione = null;
        String tokenapplicazione = null;

        String uuidUploadedFile = null;
        File createdFile = null;
        String receivedFileName = null;

        String idFascicolo = null;
        InputStream is = null;

        //creo la temp
        File tempDir = new File(System.getProperty("java.io.tmpdir"));

        if (!tempDir.exists()) {
            tempDir.mkdir();
        }

        //File tempDir = new File("C:/prova");
        //Richiesta multipart che in questo caso contiene il file e la stringa dell'idFascicolo
        try {
            idapplicazione = UtilityFunctions.getMultipartStringParam(request, "idapplicazione");
            if (idapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "part \"idapplicazione\" non trovata");
                return;
            }
            tokenapplicazione = UtilityFunctions.getMultipartStringParam(request, "tokenapplicazione");
            if (tokenapplicazione == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "part \"tokenapplicazione\" non trovata");
                return;
            }
            idFascicolo = UtilityFunctions.getMultipartStringParam(request, "idfascicolo");
            if (idFascicolo == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "part \"idfascicolo\" non trovata");
                return;
            }
            try {
                Part filePart = request.getPart("file");
                is = filePart.getInputStream();
                createdFile = File.createTempFile(getClass().getSimpleName() + "_", null, tempDir);
                createFile(is, createdFile);
                receivedFileName = filePart.getSubmittedFileName();
                IOUtils.closeQuietly(is);
            } catch (Exception ex) {
                if (createdFile != null) {
                    createdFile.delete();
                }
                throw ex;
            }

        } catch (Exception ex) {
            log.error(ex);
            throw ex;
        } catch (Error er) {
            log.error(er);
            throw er;
        } finally {
            IOUtils.closeQuietly(is);
        }

        log.info("Dati ricevuti: ");
        log.info("idFascicolo: " + idFascicolo);
        log.info("received: " + receivedFileName);

        if (idapplicazione == null || tokenapplicazione == null) {
            String message = "Dati di autenticazione errati, specificare i parametri \"idapplicazione\" e \"tokenapplicazione\" nella richiesta";
            log.error(message);

            if (createdFile != null) {
                createdFile.delete();
            }

            throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, message);

        } else if (idFascicolo == null) {
            String message = "idfascicolo non passato.";
            log.error(message);

            if (createdFile != null) {
                createdFile.delete();
            }

            throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, message);

        } else if (receivedFileName == null) {
            String message = "file non passato.";
            log.error(message);

            if (createdFile != null) {
                createdFile.delete();
            }

            throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, message);
        }

        //Carico il file su Mongo
        try {
            dbConn = UtilityFunctions.getDBConnection();

            // controllo se l'applicazione è autorizzata
            String prefix;
            try {
                prefix = UtilityFunctions.checkAuthentication(dbConn, idapplicazione, tokenapplicazione);
            } catch (NotAuthorizedException ex) {
                try {
                    dbConn.close();
                } catch (Exception subEx) {
                }
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                return;
            }

            currentDate = new Timestamp(System.currentTimeMillis());

            GregorianCalendar calendar = new GregorianCalendar();
            int year = calendar.get(GregorianCalendar.YEAR);
            int month = calendar.get(GregorianCalendar.MONTH) + 1;
            int day = calendar.get(GregorianCalendar.DAY_OF_MONTH);
            int hour = calendar.get(GregorianCalendar.HOUR_OF_DAY);
            int minute = calendar.get(GregorianCalendar.MINUTE);
            int second = calendar.get(GregorianCalendar.SECOND);
            int millisecond = calendar.get(GregorianCalendar.MILLISECOND);

            String prefixSeparator = "_";

            String[] datiFascicolo = getNomeFascicolo(idFascicolo);
            String nomeFascicolo = datiFascicolo[0];
            if (nomeFascicolo == null || nomeFascicolo.equals("")) {
                String message = "Fascicolo " + idFascicolo + " non trovato";
                throw new RequestException(HttpServletResponse.SC_NOT_FOUND, message);
            }
            String annoFascicolo = datiFascicolo[2];
            String pathMongoNumerazioneGerarchica = getGerarchiaFascicoli(datiFascicolo[1], annoFascicolo);

            String fileName = UtilityFunctions.removeExtensionFromFileName(receivedFileName);
            String fileExt = UtilityFunctions.getExtensionFromFileName(receivedFileName);
            String endFileExt = "";

            // il nome del file sarà : giorno_mese_anno_ore_minuti_secondi_nomedelfilericevuto
            if (fileExt != null) {
                endFileExt = "." + fileExt;
            }

            String mongoUri = ApplicationParams.getMongoRepositoryUri();
            boolean useMinIO = ApplicationParams.getUseMinIO();
            JSONObject minIOConfig = ApplicationParams.getMinIOConfig();
            String codiceAzienda = ApplicationParams.getCodiceAzienda();
            Integer maxPoolSize = Integer.parseInt(minIOConfig.get("maxPoolSize").toString());
            mongo = MongoWrapper.getWrapper(
                    useMinIO,
                    mongoUri,
                    minIOConfig.get("DBDriver").toString(),
                    minIOConfig.get("DBUrl").toString(),
                    minIOConfig.get("DBUsername").toString(),
                    minIOConfig.get("DBPassword").toString(),
                    codiceAzienda,
                    maxPoolSize,
                    null);

            //Creo la cartella dove inserire i file su mongo-- è quella del fascicolo passato
            String cartellaFascicolo = ApplicationParams.getUploadGdDocMongoPath() + "/" + pathMongoNumerazioneGerarchica;

            boolean exists = true;

            while (exists) {
                //fileNameToCreate = year + prefixSeparator + nomeFascicolo + prefixSeparator + fileName + prefixSeparator + generateKey(10) + endFileExt;
                fileNameToCreate = fileName + getSuffissoNomeFile(idapplicazione) + prefixSeparator + year + prefixSeparator + mettiZeroDavanti(month) + prefixSeparator + mettiZeroDavanti(day)
                        + prefixSeparator + mettiZeroDavanti(hour) + prefixSeparator + mettiZeroDavanti(minute) + prefixSeparator + mettiZeroDavanti(second)
                        + prefixSeparator + mettiZeroDavanti(millisecond) + endFileExt;

                exists = mongo.existsObjectbyPath(cartellaFascicolo + "/" + fileNameToCreate);
            }

            File newFile = new File(createdFile.getParentFile(), fileNameToCreate);
//                log.info(createdFile.getAbsolutePath());
//                log.info(newFile.getAbsolutePath());
            if (createdFile.renameTo(newFile)) {
                createdFile = newFile;
            } else {
                throw new ServletException("Errore nella rinominazione del file");
            }

            //**************************************
            //PARTE DI CARICAMENTO SU MONGO
            //**************************************
            uuidUploadedFile = mongo.put(createdFile, createdFile.getName(), cartellaFascicolo, true);
            log.info("cartella del fascicolo su mongo: " + cartellaFascicolo);

            //Disabilito l'autoCommit per fare il rollback in caso fallisca l'inserimento
            dbConn.setAutoCommit(false);

            //Eseguo gli inserimenti
            boolean okInsertGddoc = insertGdDoc(fileNameToCreate, uuidUploadedFile);
            boolean okInsertSottoDoc = insertSottoDocumento(idGdDocInserito, fileNameToCreate, uuidUploadedFile);
            boolean okInsertCross = insertFascicoliGddocCross(idGdDocInserito, idFascicolo);

            if (okInsertCross == true && okInsertGddoc == true && okInsertSottoDoc == true) {
                dbConn.commit();
            } else {
                dbConn.rollback();
                throw new ServletException("Errore nell'inserimento dei dati nella tabella di cross fascicoli_gddocs");
            }

        } catch (Exception ex) {
            log.fatal("Errore", ex);
            log.info("dati ricevuti:");
            log.info("idapplicazione: " + idapplicazione);
            log.info("id fascicolo: " + idFascicolo);

            if (uuidUploadedFile != null) {
                try {
                    mongo.delete(uuidUploadedFile);
                } catch (MongoWrapperException subEx) {
                    log.warn("errore nella cancellazione del file con uuid: " + uuidUploadedFile, subEx);
                }
            }
            try {
                if (!dbConn.getAutoCommit()) {
                    dbConn.rollback();
                }
            } catch (SQLException sqlEx) {
                ex = sqlEx;
            }

            if (ex instanceof RequestException) {
                throw (RequestException) ex;
            } else {
                throw new ServletException(ex);
            }

        } finally {
            try {
                dbConn.close();

                if (createdFile != null) {
                    createdFile.delete();
                }

            } catch (SQLException ex) {
                throw new ServletException(ex);
            }
        }

        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        try {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet " + getClass().getSimpleName() + "</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet " + getClass().getSimpleName() + " at " + request.getContextPath() + "</h1>");
            out.println("</body>");
            out.println("</html>");
        } finally {
            out.close();
        }
    }

    private void handleRequestException(RequestException requestException, HttpServletResponse response) {
        try {
            response.setStatus(requestException.getHttpStatusCode());
            response.setContentType("plain/text");
            response.getWriter().print(requestException.getMessage());
            //response.sendError(requestException.getHttpStatusCode(), requestException.getMessage());
        } catch (IOException ex) {
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
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            processRequest(request, response);
        } catch (RequestException ex) {
            handleRequestException(ex, response);
        } catch (ServletException ex) {
            RequestException requestException = new RequestException(500, ex);
            handleRequestException(requestException, response);
        }
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
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            processRequest(request, response);
        } catch (RequestException ex) {
            handleRequestException(ex, response);
        } catch (ServletException ex) {
            RequestException requestException = new RequestException(500, ex);
            handleRequestException(requestException, response);
        }
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

    private String[] getNomeFascicolo(String idFascicolo) throws SQLException {

        String[] campiFascicolo = new String[3];

        String parametersTable = ApplicationParams.getFascicoliTableName();
        String query = "SELECT nome_fascicolo, numerazione_gerarchica, anno_fascicolo FROM " + parametersTable + " WHERE id_fascicolo = ? ";

        PreparedStatement ps = dbConn.prepareStatement(query);
        ps.setString(1, idFascicolo);

        log.info("esegui la query: " + ps.toString());
        ResultSet result = ps.executeQuery();

        if (result != null && result.next() == true) {
            campiFascicolo[0] = result.getString(1);
            campiFascicolo[1] = result.getString(2);
            campiFascicolo[2] = result.getString(3);
        }

        return campiFascicolo;
    }

    private String getGerarchiaFascicoli(String numerazioneGerarchica, String anno) {
        int pos = numerazioneGerarchica.indexOf('/');

        String numerazioneSenzaAnno = numerazioneGerarchica.substring(0, pos);

        //Devo controllare se è il fascicolo speciale. Se inizia con il "-1" allora il primo "-" non devo toglierlo
//        String gerarchia = null;
//        System.out.println("asfasf: " + anno + "_" + numerazioneSenzaAnno.replace("-", "/"));
        return anno + "_" + numerazioneSenzaAnno.replace("-", "/");
//        if(ger.equals("-1"))
//        {
//            String s = numerazioneSenzaAnno.substring(3);
//           // 2-54-26
//           // -1-54-26
//           // 2
//            //-1
//            // 2-54
//            // -1-2
//            // -1-2-54
//            
//            //2014_-1/54/26
//            
//            s = "/" + anno + "_" + ger + "/" + s.replace("-", "/");
//            System.out.println(s);
//            gerarchia = s;
//        }
//        else
//        {
//            gerarchia = "/" + anno + "_" + numerazioneSenzaAnno.replace("-","/" );
//        } 
//                    
//       // gerarchia = numerazioneSenzaAnno.replace("-","/" );

//        System.out.println("gerarchia " + gerarchia);
//        return gerarchia;
    }

    private String generateKey(int lenght) {
        char[] chars = "abcdefghijklmnopqrstuvwxyzABSDEFGHIJKLMNOPQRSTUVWXYZ1234567890".toCharArray();
        Random r = new Random();
        char[] id = new char[lenght];
        for (int i = 0; i < id.length; i++) {
            id[i] = chars[r.nextInt(chars.length)];
        }
        return new String(id);
    }

    private Boolean insertGdDoc(String nomeGddoc, String uuidUploadFile) {

        String gdDocsTable = ApplicationParams.getGdDocsTableName();
        String query = "INSERT INTO " + gdDocsTable + " (id_gddoc, nome_gddoc, multiplo, uuid_mongo, tipo_gddoc, uuid_mongo_pdf,"
                + " stato_gd_doc, data_gddoc) VALUES (?,?,?,?,?,?,?,?)";

        idGdDocInserito = generateKey(20);

        PreparedStatement ps;

        try {
            ps = dbConn.prepareStatement(query);
            ps.setString(1, idGdDocInserito);
            ps.setString(2, nomeGddoc);
            ps.setInt(3, 0);
            ps.setString(4, uuidUploadFile);
            ps.setString(5, "d");
            ps.setString(6, uuidUploadFile);
            ps.setInt(7, 1);
            ps.setTimestamp(8, currentDate);

            log.info("eseguo la query: " + ps.toString() + "...");

            int rows = ps.executeUpdate();

            //Ritorno false perchè non ha inserito nulla
            if (rows == 0) {
                return false;
            }

        } catch (SQLException ex) {
            log.error(ex);

            if (ex.getSQLState().startsWith(SQL_EXCEPTION_DUPLICATED_ITEM)) {
                insertGdDoc(nomeGddoc, uuidUploadFile);
            } else {
                return false;
            }
        }

        return true;
    }

    private Boolean insertSottoDocumento(String idGdDoc, String nomeFile, String uuidUploaFile) {
        String sottoDocumentiTable = ApplicationParams.getSottoDocumentiTableName();
        String query = "INSERT INTO " + sottoDocumentiTable + "(id_sottodocumento, id_gddoc, nome_sottodocumento, uuid_mongo_pdf, uuid_mongo_originale, codice_sottodocumento) "
                + " VALUES(?,?,?,?,?,?) ";

        String idSottoDocumento = generateKey(20);

        PreparedStatement ps;

        try {
            ps = dbConn.prepareStatement(query);
            ps.setString(1, idSottoDocumento);
            ps.setString(2, idGdDoc);
            ps.setString(3, nomeFile);
            ps.setString(4, uuidUploaFile);
            ps.setString(5, uuidUploaFile);
            ps.setString(6, "babel_" + idSottoDocumento);

            log.info("eseguo la query: " + ps.toString() + "...");

            int rows = ps.executeUpdate();

            //Ritorno false perchè non ha inserito nulla
            if (rows == 0) {
                return false;
            }

        } catch (SQLException ex) {
            log.error(ex);

            if (ex.getSQLState().startsWith(SQL_EXCEPTION_DUPLICATED_ITEM)) {
                insertSottoDocumento(idGdDoc, nomeFile, uuidUploaFile);
            } else {
                return false;
            }
        }

        return true;
    }

    private Boolean insertFascicoliGddocCross(String idGddoc, String idFascicolo) {
        String crossFascGdDocTable = ApplicationParams.getFascicoliGdDocsTableName();
        String query = "INSERT INTO " + crossFascGdDocTable + "(id_fascicolo_gddoc, id_gddoc, id_fascicolo, visibile, data_assegnazione, conservazione) "
                + " VALUES(?,?,?,?,?,?) ";

        String idFascGddoc = generateKey(20);

        PreparedStatement ps;

        try {
            ps = dbConn.prepareStatement(query);
            ps.setString(1, idFascGddoc);
            ps.setString(2, idGddoc);
            ps.setString(3, idFascicolo);
            ps.setInt(4, -1);
            ps.setTimestamp(5, currentDate);
            ps.setInt(6, -1);

            log.info("eseguo la query: " + ps.toString() + "...");

            int rows = ps.executeUpdate();

            //Ritorno false perchè non ha inserito nulla
            if (rows == 0) {
                return false;
            }

        } catch (SQLException ex) {
            log.error(ex);

            if (ex.getSQLState().startsWith(SQL_EXCEPTION_DUPLICATED_ITEM)) {
                insertFascicoliGddocCross(idGddoc, idFascicolo);
            } else {
                return false;
            }
        }

        return true;
    }

    private File createFile(InputStream is, File fileToCreate) throws FileNotFoundException, IOException {
        FileOutputStream fos = null;

        try {

            fos = new FileOutputStream(fileToCreate);
            byte[] readData = new byte[1024];
            int i = is.read(readData);
            while (i != -1) {
                fos.write(readData, 0, i);
                i = is.read(readData);
            }
            is.close();
            fos.close();
            return fileToCreate;
        } finally {

            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    private String mettiZeroDavanti(int numero) {
        String numeroString;

        if (numero < 10) {
            numeroString = "0" + numero;
        } else {
            numeroString = numero + "";
        }

        return numeroString;
    }

    private String getSuffissoNomeFile(String idApplicazione) {
        String suffisso = "";

        if (null != idApplicazione) {
            switch (idApplicazione) {
                case "procton":
                    suffisso = "_Pico";
                    break;
                case "dete":
                    suffisso = "_Dete";
                    break;
                case "deli":
                    suffisso = "_Deli";
                    break;
            }
        }

        return suffisso;
    }

}
