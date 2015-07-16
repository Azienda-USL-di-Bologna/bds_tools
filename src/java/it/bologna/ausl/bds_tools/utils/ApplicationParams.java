package it.bologna.ausl.bds_tools.utils;

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
    private static String publicParametersTableName;
    private static String redisInQueue;
    private static String authenticationTable;
    private static int resourceLockedMaxRetryTimes;
    private static long resourceLockedSleepMillis;
    
    public static void initApplicationParams(ServletContext context) throws SQLException, NamingException, ServletException {
        try (Connection dbConn = UtilityFunctions.getDBConnection()) {
            appId = context.getInitParameter("appid");
            appToken = context.getInitParameter("apptoken");
            publicParametersTableName = context.getInitParameter("ParametersTableName");
            resourceLockedMaxRetryTimes = Integer.parseInt(context.getInitParameter("ResourceLockedMaxRetryTimes"));
            resourceLockedSleepMillis = Long.parseLong(context.getInitParameter("resourceLockedSleepMillis"));
            
            readAuthenticationTable(context);
            initilizeSupporetdFiles(dbConn, context);
            serverId = serverId = UtilityFunctions.getPubblicParameter(dbConn, "serverIdentifier");
            //mongoUri = context.getInitParameter("mongo" + serverId);
            mongoUri = UtilityFunctions.getPubblicParameter(dbConn, "mongoConnectionString");
            //redisHost = context.getInitParameter("redis" + serverId);
            redisHost = UtilityFunctions.getPubblicParameter(dbConn, "masterChefHost");
            //redisInQueue = context.getInitParameter("redisinqueue" + serverId);
            redisInQueue = UtilityFunctions.getPubblicParameter(dbConn, "masterChefPushingQueue");
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

    public static String getPublicParametersTableName() {
        return publicParametersTableName;
    }

    public static void setPublicParametersTableName(String publicParametersTableName) {
        ApplicationParams.publicParametersTableName = publicParametersTableName;
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

    public static int getResourceLockedMaxRetryTimes() {
        return resourceLockedMaxRetryTimes;
    }

    public static void setResourceLockedMaxRetryTimes(int resourceLockedMaxRetryTimes) {
        ApplicationParams.resourceLockedMaxRetryTimes = resourceLockedMaxRetryTimes;
    }

    public static long getResourceLockedSleepMillis() {
        return resourceLockedSleepMillis;
    }

    public static void setResourceLockedSleepMillis(long resourceLockedSleepMillis) {
        ApplicationParams.resourceLockedSleepMillis = resourceLockedSleepMillis;
    }
}
