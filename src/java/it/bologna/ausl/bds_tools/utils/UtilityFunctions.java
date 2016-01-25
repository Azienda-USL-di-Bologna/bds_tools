package it.bologna.ausl.bds_tools.utils;

/**
 *
 * @author Giuseppe De Marco (gdm)
 */
import it.bologna.ausl.bds_tools.exceptions.ConvertPdfExeption;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.masterchefclient.PdfConvertParams;
import it.bologna.ausl.masterchefclient.PdfConvertResult;
import it.bologna.ausl.masterchefclient.WorkerData;
import it.bologna.ausl.masterchefclient.WorkerResponse;
import it.bologna.ausl.masterchefclient.WorkerResult;
import it.bologna.ausl.mimetypeutility.Detector;
import it.bologna.ausl.redis.RedisClient;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import javax.sql.DataSource;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.mime.MimeTypeException;

public class UtilityFunctions {

private static final Logger log = LogManager.getLogger(UtilityFunctions.class);

//private static Context initContext;
private static  DataSource ds;

static {
    try {
//        initContext = new InitialContext();
//        Context envContext = (Context) initContext.lookup("java:/comp/env");
        ds = (DataSource) ((Context) new InitialContext().lookup("java:/comp/env")).lookup("jdbc/argo");
    }
    catch (Exception ex) {
        log.error(ex);
    }
}

    public static Connection getDBConnection() throws SQLException, NamingException {
//        if (initContext == null)
//            initContext = new InitialContext();
//        Context envContext = (Context) initContext.lookup("java:/comp/env");
//        DataSource ds = (DataSource) envContext.lookup("jdbc/argo");
//        Connection conn = ds.getConnection();
        return ds.getConnection();
    }
    
    public static DataSource getDataSource() {
        return ds;
    }

    /**
     * Controlla se l'applicazione è autorizzata e ne ritorna il prefisso da usare nella costruzione degli id
     * @param dbConn connessione
     * @param idApplicazione id applicazione della quale verificare l'autenticazione
     * @param token token corrispondente all'id applicazione della quale verificare l'autenticazione
     * @return se l'applicazione è autorizzata torna il prefisso da usare nella costruzione degli id, se non è autorizzata torna NotAuthorizedException
     * @throws it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException se l'applicazione non è autorizzata
     * @throws java.sql.SQLException
     */
    public static String checkAuthentication(Connection dbConn, String idApplicazione, String token) throws NotAuthorizedException, SQLException {
        String sqlText = 
                    "SELECT prefix " +
                    "FROM " + ApplicationParams.getAuthenticationTable() + " " +
                    "WHERE id_applicazione = ? and token = ?";

        PreparedStatement ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idApplicazione);
        ps.setString(2, token);
        String query = ps.toString().substring(0, ps.toString().lastIndexOf("=") + 1) + " ******";
        log.debug("eseguo la query: " + query + " ...");
//            dbConn.setAutoCommit(true);
        ResultSet authenticationResultSet = ps.executeQuery();

        if (authenticationResultSet.next() == false) {
            String message = "applicazione: " + idApplicazione + " non autorizzata";
            log.error(message);
            throw new NotAuthorizedException(message);
        } else {
            String message = "applicazione: " + idApplicazione + " autorizzata";
            log.info(message);
            return authenticationResultSet.getString(1);
        }
    }

    public static String getPubblicParameter(Connection dbConn, String parameterName) throws SQLException {
        String query = "SELECT val_parametro FROM " + ApplicationParams.getPublicParametersTableName() + " WHERE nome_parametro = ?";
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {

            ps.setString(1, parameterName);

            ResultSet result = ps.executeQuery();
            String value = null;

            if (result != null && result.next() == true) {
                value = result.getString(1);
            }
            return value;
        }
    }
    
    public static String arrayToString(Object[] array, String separator) {
        StringBuilder builder = new StringBuilder();
        if (array == null || array.length == 0) {
            return null;
        }
        builder.append(array[0].toString());
        for (int i = 1; i < array.length; i++) {
            builder.append(separator).append(array[i].toString());
        }
        return builder.toString();
    }

    public static String getExtensionFromFileName(String fileName) {
        String res = "";
        int pos = fileName.lastIndexOf(".");
        if (pos > 0) {
            res = fileName.substring(pos + 1, fileName.length());
        }
        return res;
    }

    public static String removeExtensionFromFileName(String fileName) {
        String res = fileName;
        int pos = fileName.lastIndexOf(".");
        if (pos > 0) {
            res = res.substring(0, pos);
        }
        return res;
    }

    public static String getLocalUrl(HttpServletRequest request) {
        String url =
                request.getScheme()
                + "://"
                + request.getServerName()
                + ("http".equals(request.getScheme()) && request.getServerPort() == 80 || "https".equals(request.getScheme()) && request.getServerPort() == 443 ? "" : ":" + request.getServerPort());
        return url;
    }
    
        /**
     * Converte un InputStream in una stringa
     *
     * @param is l'InputStream da convertire
     * @return L'inputStream convertito in stringa
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    public static String inputStreamToString(InputStream is) throws UnsupportedEncodingException, IOException {
        Writer stringWriter = new StringWriter();
        char[] buffer = new char[1024];
        try {
            Reader reader = new BufferedReader(new InputStreamReader(is, Charsets.UTF_8));
            int n;
            while ((n = reader.read(buffer)) != -1) {
                stringWriter.write(buffer, 0, n);
            }
        } finally {
        }
        return stringWriter.toString();
    }

    /**
     *  Torna un inputStream della stringa passata
     * @param str
     * @return 
     */
    public static InputStream stringToInputStream(String str) {
        try {
            InputStream is = new ByteArrayInputStream(str.getBytes(Charsets.UTF_8));
            return is;
        } catch (Exception ex) {
            //ex.printStackTrace(System.out);
            return null;
        }
    }
    
    public static String sendHttpMessage(String targetUrl, String username, String password, Map<String, byte[]> parameters, String method, String contentType) throws MalformedURLException, IOException, SendHttpMessageException {

        if (contentType == null)
            contentType = "application/x-www-form-urlencoded";
        //System.out.println("connessione...");		
        String textParameters = "";
        byte[] byteParameters = null;
        int contentLength = 0;
        if (parameters != null) {   
            if (contentType.equals("application/x-www-form-urlencoded")) {
                Set<Map.Entry<String, byte[]>> entrySet = parameters.entrySet();
                Iterator<Map.Entry<String, byte[]>> iterator = entrySet.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, byte[]> param = iterator.next();
                    String paramName = param.getKey();
                    String paramValue = new String(param.getValue());
                    textParameters += paramName + "=" + paramValue;
                    if (iterator.hasNext()) {
                        textParameters += "&";
                    }
                }
                textParameters = textParameters.replace(" ", "%20");
                contentLength = textParameters.getBytes().length;
            }
            else {
                byteParameters = parameters.values().iterator().next();
                contentLength = byteParameters.length;
            }
        }
        URL url = new URL(targetUrl);
        method = method.toUpperCase();
        if (method.equals("GET") || method.equals("DELETE")) {
            if (textParameters.length() > 0) {
                targetUrl += "?" + textParameters;
            }
            url = new URL(targetUrl);
        }
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        if (username != null && !username.equals("")) {
            String userpassword;
            if (password != null) {
                userpassword = username + ":" + password;
            } else {
                userpassword = "restuser";
            }
            String encodedAuthorization = Base64Coder.encodeString(userpassword);
            connection.setRequestProperty("Authorization", "Basic " + encodedAuthorization);
        }

        if (method.equals("POST")) {
            connection.setDoOutput(true);
        }
        connection.setDoInput(true);
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod(method);
        connection.setRequestProperty("Content-Type", contentType);
        if (contentType.equals("application/x-www-form-urlencoded")) {
            connection.setRequestProperty("charset", "utf-8");
        }
        if (method.equals("POST")) {
            connection.setRequestProperty("Content-Length", "" + Integer.toString(contentLength));
        }
        connection.setUseCaches(false);

        if (method.equals("POST")) {
             DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            if (!textParameters.equals("")) {
                wr.writeBytes(textParameters);
                wr.flush();
                wr.close();
            }
            else {
                wr.write(byteParameters);
            }
        }

        int responseCode = connection.getResponseCode();
        String responseCodeToString = String.valueOf(responseCode);
        if (!responseCodeToString.substring(0, responseCodeToString.length() - 1).equals("20")) {
            throw new SendHttpMessageException(responseCode, inputStreamToString(connection.getErrorStream()));
        }
        
        InputStream resultStream = connection.getInputStream();
        //System.out.println("risposta: " + responseCode + " - " + connection.getResponseMessage());

        String resultString = null;
        if (resultStream != null) 
            resultString = inputStreamToString(resultStream);

        IOUtils.closeQuietly(resultStream);
        connection.disconnect();
        return resultString;
    }
    
    public static String getMultipartStringParam(HttpServletRequest request, String paramName) {
        InputStream is = null;
        String param = null;
        try {
            Part part = request.getPart(paramName);
            is = part.getInputStream();
            param = UtilityFunctions.inputStreamToString(is);
        }
        catch (Exception ex) {
            log.error(ex);
        }
        finally {
            IOUtils.closeQuietly(is);
        }
        return param;
    }
    
    /** Chidede al masterchef la conversione di un file in pdf
     * 
     * @param uuid l'uuid su mongo del file da convertire
     * @param mongoPath il mongopath in cui scrivere il file
     * @param mimeType il mimeType del file da convertire
     * @return l'uuid del file convertito
     * @throws it.bologna.ausl.bds_tools.exceptions.ConvertPdfExeption
     */
    public static String convertPdf(String uuid, String mongoPath, String mimeType) throws ConvertPdfExeption {
        if (isConvertibilePdf(mimeType)) {
            try {
                PdfConvertParams pdfConvertJobParams = new PdfConvertParams(uuid, mongoPath, true);  
                String retQueue = uuid + "_" + ApplicationParams.getAppId() + "_pdfConvertRetQueue_" + ApplicationParams.getServerId();
                WorkerData wd = new WorkerData(ApplicationParams.getAppId(), "1", retQueue);
                wd.addNewJob("1", null, pdfConvertJobParams);
                RedisClient rd = new RedisClient(ApplicationParams.getRedisHost(), null);
                rd.put(wd.getStringForRedis(), ApplicationParams.getRedisInQueue());
                
                String res = rd.bpop(retQueue, 86400);
                WorkerResponse wr = new WorkerResponse(res);
                if (wr.getStatus().equalsIgnoreCase("ok")) {
                    WorkerResult result = wr.getResult(0);
                    PdfConvertResult pdfConvertResult = (PdfConvertResult) result.getRes();
                    String convertedUuid = pdfConvertResult.getUUIDConvertedFile();
                    if (convertedUuid != null && !convertedUuid.equals("")) 
                        return convertedUuid;
                    else 
                        throw new ConvertPdfExeption("uuid nullo");
                }
                else {
                    throw new ConvertPdfExeption("errore tornato dal masterchef: " + wr.getError());
                }
            }
            catch (Exception ex) {
               if (ex instanceof ConvertPdfExeption)
                   throw (ConvertPdfExeption)ex;
               else
                throw new ConvertPdfExeption(ex);
            }
        }
        else
            throw new ConvertPdfExeption("i file di tipo: " + mimeType + " non sono convertibili in pdf");
    }
    
    public static boolean isConvertibilePdf(String mimeType) {
        try {
            return SupportedFile.getSupportedFile(ApplicationParams.getSupportedFileList(), mimeType).isPdfConvertible();
        }
        catch (Exception ex) {
            log.error(ex);
            return false;
        }
    }

    public static boolean isSupportedFile(File file) throws SQLException, IOException, UnsupportedEncodingException, MimeTypeException {
        FileInputStream fis = new FileInputStream(file);
        try {
            return isSupportedFile(new FileInputStream(file));
        }
        finally {
            IOUtils.closeQuietly(fis);
        }
    }
    
    public static boolean isSupportedFile(InputStream is) throws SQLException, IOException, UnsupportedEncodingException, MimeTypeException {
        Detector d = new Detector();
        return SupportedFile.isSupported(ApplicationParams.getSupportedFileList(), d.getMimeType(is));
    }
    
}
