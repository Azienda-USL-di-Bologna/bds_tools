package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.exceptions.RequestException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.mongowrapper.MongoWrapper;
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
import java.util.List;
import java.util.Random;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author Andrea
 */
public class UploadGdDocInFascicolo extends HttpServlet {

    private static final Logger log = Logger.getLogger(UploadGdDocInFascicolo.class);
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
        PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource("it/bologna/ausl/bds_tools/conf/log4j.properties"));
        // configuro il logger per la console
        BasicConfigurator.configure();

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

        //creo la temp
        
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        //File tempDir = new File("C:/prova");
        //Richiesta multipart che in questo caso contiene il file e la stringa dell'idFascicolo
        if (ServletFileUpload.isMultipartContent(request)) {
            ServletFileUpload sfu = new ServletFileUpload(new DiskFileItemFactory(1024 * 1024, tempDir));

            List fileItems = null;

            try {
                fileItems = sfu.parseRequest(request);

            } catch (FileUploadException ex) {
                throw new ServletException(ex);
            }
            for (int elementIndex = 0; elementIndex < fileItems.size(); elementIndex++) {

                FileItem item = (FileItem) fileItems.get(elementIndex);
                if (item.isFormField() && item.getFieldName().equals("idfascicolo")) {
                    idFascicolo = item.getString();
                } else if (item.isFormField() && item.getFieldName().equals("idapplicazione")) {
                    idapplicazione = item.getString();
                } else if (item.isFormField() && item.getFieldName().equals("tokenapplicazione")) {
                    tokenapplicazione = item.getString();
                } else if (!item.isFormField() && item.getFieldName().equals("file") && item.getSize() > 0) {
                    try {
                        
                        createdFile = File.createTempFile(getClass().getSimpleName() + "_", null, tempDir);
                        createFile(item.getInputStream(), createdFile);
                        receivedFileName = item.getName();
                        
                    } catch (Exception ex) {
                        
                        if(createdFile != null)
                            createdFile.delete();
                                                        
                        throw new ServletException(ex);
                    }
                }
            }

        } else {
            response.getWriter().print("Il servizio supporta solo richieste multipart");
            response.setStatus(HttpServletResponse.SC_NOT_ACCEPTABLE);
            return;
        }

        log.info("Dati ricevuti: ");
        log.info("idFascicolo: " + idFascicolo);
        log.info("received: " + receivedFileName);
        
        if (idapplicazione == null || tokenapplicazione == null) {
            String message = "Dati di autenticazione errati, specificare i parametri \"idapplicazione\" e \"tokenapplicazione\" nella richiesta";
            log.error(message);
            
            if(createdFile != null)
                createdFile.delete();        

            throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, message);
            
        }
        else if (idFascicolo == null) 
        {
            String message = "idfascicolo non passato.";
            log.error(message);
            
            if(createdFile != null)
                createdFile.delete();        
                        
            throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, message);
            
        }
        else if (receivedFileName == null) {
            String message = "file non passato.";
            log.error(message);
            
            if(createdFile != null)
                createdFile.delete();        
            
            throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, message);
        }

       //Carico il file su Mongo
        try {
            // dati per l'autenticazione
            
            String authenticationTable = getServletContext().getInitParameter("AuthenticationTable");

            dbConn = UtilityFunctions.getDBConnection();      
                    
            if (!UtilityFunctions.checkAuthentication(dbConn, authenticationTable, idapplicazione, tokenapplicazione)) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
                try {
                    dbConn.close();
                }
                catch (Exception ex) {
                }
                
                if(createdFile != null )
                    createdFile.delete();        

                String message = "Accesso negato";
                throw new RequestException(HttpServletResponse.SC_FORBIDDEN, message);
            }
            else 
            {
                
                currentDate = new Timestamp(System.currentTimeMillis());

                GregorianCalendar calendar = new GregorianCalendar();
                int year = calendar.get(GregorianCalendar.YEAR);
                String prefixSeparator = "_";

                String[] datiFascicolo = getNomeFascicolo(idFascicolo);
                String nomeFascicolo = datiFascicolo[0];
                if (nomeFascicolo == null || nomeFascicolo.equals("")) 
                {
                    String message = "Fascicolo " + idFascicolo + " non trovato";
                    throw new RequestException(HttpServletResponse.SC_NOT_FOUND, message);
                }
                String annoFascicolo = datiFascicolo[2];
                String pathMongoNumerazioneGerarchica = getGerarchiaFascicoli(datiFascicolo[1], annoFascicolo);

                String fileName = UtilityFunctions.removeExtensionFromFileName(receivedFileName);
                String fileExt = UtilityFunctions.getExtensionFromFileName(receivedFileName);
                String endFileExt = "";
                
                // il nome del file sarà : giorno_mese_anno_ore_minuti_secondi_nomedelfilericevuto
                if(fileExt != null)
                    endFileExt="." + fileExt;
                                   
                String serverId = getParameterValue("serverIdentifier");
                String mongoUri = getServletContext().getInitParameter("mongo" + serverId);
                mongo = new MongoWrapper(mongoUri);

                //Creo la cartella dove inserire i file su mongo-- è quella del fascicolo passato
                String cartellaFascicolo = getServletContext().getInitParameter("UploadGdDocMongoPath") + pathMongoNumerazioneGerarchica;
                
                boolean exists = true;
                
                while(exists)
                {
                    fileNameToCreate = year + prefixSeparator + nomeFascicolo + prefixSeparator + fileName + prefixSeparator + generateKey(10) + endFileExt;
                    exists = mongo.existsObjectbyPath(cartellaFascicolo + "/" + fileNameToCreate);                          
                }
                
                File newFile = new File(createdFile.getParentFile(), fileNameToCreate);
//                log.info(createdFile.getAbsolutePath());
//                log.info(newFile.getAbsolutePath());
                if(createdFile.renameTo(newFile))
                {
                    createdFile= newFile;
                }
                else
                {
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
               
                              
                if(okInsertCross == true && okInsertGddoc == true && okInsertSottoDoc == true)
                {
                    dbConn.commit();
                }
                else
                {
                    dbConn.rollback();
                }
            }
        }
        catch (Exception ex) {
            log.fatal("Errore", ex);
            log.info("dati ricevuti:");
            log.info("idapplicazione: " + idapplicazione);
            log.info("id fascicolo: " + idFascicolo);

            if (uuidUploadedFile != null) {
                mongo.delete(uuidUploadedFile);
            }
            try 
            {
                if(!dbConn.getAutoCommit())
                    dbConn.rollback();
            } 
            catch (SQLException sqlEx) 
            {
                ex = sqlEx;
            }

            if (ex instanceof RequestException)
                throw (RequestException) ex;
            else
                throw new ServletException(ex);

        }
        finally {
            try {
                dbConn.close();
                
                if(createdFile != null)
                    createdFile.delete();        
                
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
            out.println("<title>Servlet SetGddocAndFascicoloSpeciale</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet SetGddocAndFascicoloSpeciale at " + request.getContextPath() + "</h1>");
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
        }
        catch (IOException ex) {
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
        }
        catch (RequestException ex) {
            handleRequestException(ex, response);
        }
        catch (ServletException ex) {
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
        }
        catch (RequestException ex) {
            handleRequestException(ex, response);
        }
        catch (ServletException ex) {
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

    private String getParameterValue(String parameterName) throws SQLException {
        String parametersTable = getServletContext().getInitParameter("ParametersTableName");
        String query = "SELECT val_parametro FROM " + parametersTable + " WHERE nome_parametro = ?";
        PreparedStatement ps = dbConn.prepareStatement(query);
        ps.setString(1, parameterName);

        ResultSet result = ps.executeQuery();
        String value = null;

        if (result != null && result.next() == true) {
            value = result.getString(1);
        }
        return value;
    }

    private String[] getNomeFascicolo(String idFascicolo) throws SQLException {
        
        String[] campiFascicolo = new String[3];
        
        String parametersTable = getServletContext().getInitParameter("FascicoliTableName");
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
    
    private String getGerarchiaFascicoli(String numerazioneGerarchica, String anno)
    {
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

    private String generateKey(int lenght)
    {
        char[] chars = "abcdefghijklmnopqrstuvwxyzABSDEFGHIJKLMNOPQRSTUVWXYZ1234567890".toCharArray();
        Random r = new Random();
        char[] id = new char[lenght];
        for (int i = 0;  i < id.length;  i++) {
            id[i] = chars[r.nextInt(chars.length)];
        }
        return new String(id);
    }
    
    private Boolean insertGdDoc(String nomeGddoc, String uuidUploadFile) {
                
        String gdDocsTable = getServletContext().getInitParameter("GdDocsTableName");
        String query = "INSERT INTO " + gdDocsTable + " (id_gddoc, nome_gddoc, categoria_origine, multiplo, uuid_mongo, tipo_gddoc, uuid_mongo_pdf,"
                + " stato_gd_doc, data_gddoc) VALUES (?,?,?,?,?,?,?,?,?)";
               
        idGdDocInserito = generateKey(20);
        
        PreparedStatement ps;
              
        try 
        {
            ps = dbConn.prepareStatement(query);
            ps.setString(1,idGdDocInserito);
            ps.setString(2, nomeGddoc);
            ps.setString(3, "Upload");
            ps.setInt(4, 0);
            ps.setString(5, uuidUploadFile);
            ps.setString(6, "d");
            ps.setString(7, uuidUploadFile);
            ps.setInt(8, 1);
            ps.setTimestamp(9, currentDate);
            
            log.info("eseguo la query: " + ps.toString() + "...");
            
           int rows = ps.executeUpdate();
           
           //Ritorno false perchè non ha inserito nulla
           if(rows == 0)
               return false;
           
        } catch (SQLException ex) 
        {
            log.error(ex);
            
            if(ex.getSQLState().startsWith(SQL_EXCEPTION_DUPLICATED_ITEM))
            {
                insertGdDoc(nomeGddoc, uuidUploadFile);
            }
            else
               return false;
        }
        
        return true;
    }
    
    private Boolean insertSottoDocumento(String idGdDoc, String nomeFile, String uuidUploaFile)
    {
        String sottoDocumentiTable = getServletContext().getInitParameter("SottoDocumentiTableName");
        String query = "INSERT INTO " + sottoDocumentiTable + "(id_sottodocumento, id_gddoc, nome_sottodocumento, uuid_mongo_pdf, uuid_mongo_originale) "
                       + " VALUES(?,?,?,?,?) ";
        
        String idSottoDocumento = generateKey(20);
        
        PreparedStatement ps;
        
        try
        {
            ps = dbConn.prepareStatement(query);
            ps.setString(1,idSottoDocumento);
            ps.setString(2, idGdDoc);
            ps.setString(3, nomeFile);
            ps.setString(4, uuidUploaFile);
            ps.setString(5, uuidUploaFile);
            
            log.info("eseguo la query: " + ps.toString() + "...");
            
            int rows = ps.executeUpdate();
           
           //Ritorno false perchè non ha inserito nulla
           if(rows == 0)
               return false;
            
        }
        catch(SQLException ex)
        {
            log.error(ex);
            
            if(ex.getSQLState().startsWith(SQL_EXCEPTION_DUPLICATED_ITEM))
            {
                insertSottoDocumento(idGdDoc, nomeFile,uuidUploaFile);
            }
            else
               return false;
        }
        
        return true;
    }
    
    private Boolean insertFascicoliGddocCross(String idGddoc, String idFascicolo)
    {
        String crossFascGdDocTable = getServletContext().getInitParameter("FascicoliGdDocsTableName");
        String query = "INSERT INTO " + crossFascGdDocTable + "(id_fascicolo_gddoc, id_gddoc, id_fascicolo, visibile, data_assegnazione, conservazione) "
                       + " VALUES(?,?,?,?,?,?) ";
        
        String idFascGddoc = generateKey(20);
        
        PreparedStatement ps;
        
        try
        {
            ps = dbConn.prepareStatement(query);
            ps.setString(1,idFascGddoc);
            ps.setString(2, idGddoc);
            ps.setString(3, idFascicolo);
            ps.setInt(4, -1);
            ps.setTimestamp(5, currentDate);
            ps.setInt(6, -1);
            
            log.info("eseguo la query: " + ps.toString() + "...");
            
            int rows = ps.executeUpdate();
           
           //Ritorno false perchè non ha inserito nulla
           if(rows == 0)
               return false;
            
        }
        catch(SQLException ex)
        {
            log.error(ex);
            
            if(ex.getSQLState().startsWith(SQL_EXCEPTION_DUPLICATED_ITEM))
            {
                insertFascicoliGddocCross(idGddoc, idFascicolo);
            }
            else
               return false;
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

}