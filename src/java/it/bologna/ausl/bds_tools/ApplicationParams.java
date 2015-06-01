package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.utils.SupportedFile;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author gdm
 */
public class ApplicationParams {
    private static final Logger log = LogManager.getLogger(ApplicationParams.class);
    
    private static List<SupportedFile> supportedFileList;
    private static String appId;
    private static String appToken;
    private static String serverId;
    private static String mongoUri;
    private static String redisHost;
    private static String redisInQueue;
    private static String authenticationTable;
    
    public static void initApplicationParams(ServletContext context) throws SQLException, NamingException, ServletException {
        Connection dbConn = null;
        try {
            dbConn = UtilityFunctions.getDBConnection();
            appId = context.getInitParameter("appid");
            appToken = context.getInitParameter("apptoken");
            readAuthenticationTable(context);
            initilizeSupporetdFiles(dbConn, context);
            serverId = serverId = UtilityFunctions.getPubblicParameter(dbConn, context.getInitParameter("ParametersTableName"), "serverIdentifier");
            mongoUri = context.getInitParameter("mongo" + serverId);
            redisHost = context.getInitParameter("redis" + serverId);
            redisInQueue = context.getInitParameter("redisinqueue" + serverId);
        }
        finally {
            if (dbConn != null)
                dbConn.close();
        }
    }

    private static void readAuthenticationTable(ServletContext context) throws ServletException {
        authenticationTable = context.getInitParameter("AuthenticationTable");
        if(authenticationTable == null || authenticationTable.equals("")) {
            String message = "Manca il nome della tabella per l'autenticazione. Indicarlo nel file \"web.xml\"";
            log.error(message);
            throw new ServletException(message);
        }
    }
    private static void initilizeSupporetdFiles(Connection dbConn, ServletContext context) throws SQLException {
        // ottengo una connessione al db
        
        PreparedStatement ps = null;
        try {
            String fileSupportatiTable = context.getInitParameter("FileSupportatiTableName");
            String sqlText = "SELECT * FROM " +  fileSupportatiTable;
            ps = dbConn.prepareStatement(sqlText);

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet results = ps.executeQuery();
            List<SupportedFile> supportedFiles = new ArrayList<SupportedFile>();
            while (results.next()) {
                supportedFiles.add(new SupportedFile(results.getString("mime_type"), results.getString("estensione"), results.getBoolean("convertibile_pdf")));
            }
            ApplicationParams.setSupportedFileList(supportedFiles);
        }
        finally {
            if (ps != null)
                ps.close();
        }
    }
    
    public static List<SupportedFile> getSupportedFileList() {
        return supportedFileList;
    }

    public synchronized static void setSupportedFileList(List<SupportedFile> supportedFileList) {
        ApplicationParams.supportedFileList = supportedFileList;
    }

    public static String getAppId() {
        return appId;
    }

    public static void setAppId(String appId) {
        ApplicationParams.appId = appId;
    }

    public static String getAppToken() {
        return appToken;
    }

    public static void setAppToken(String appToken) {
        ApplicationParams.appToken = appToken;
    }

    public static String getServerId() {
        return serverId;
    }

    public static void setServerId(String serverId) {
        ApplicationParams.serverId = serverId;
    }

    public static String getMongoUri() {
        return mongoUri;
    }

    public static void setMongoUri(String mongoUri) {
        ApplicationParams.mongoUri = mongoUri;
    }

    public static String getRedisHost() {
        return redisHost;
    }

    public static void setRedisHost(String redisHost) {
        ApplicationParams.redisHost = redisHost;
    }

    public static String getRedisInQueue() {
        return redisInQueue;
    }

    public static void setRedisInQueue(String redisInQueue) {
        ApplicationParams.redisInQueue = redisInQueue;
    }

    public static String getAuthenticationTable() {
        return authenticationTable;
    }

    public static void setAuthenticationTable(String authenticationTable) {
        ApplicationParams.authenticationTable = authenticationTable;
    }
}