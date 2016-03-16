package it.bologna.ausl.bds_tools.utils;

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

    private static String registriTableName;
    private static String spedizioniPecGlobaleTableName;
    private static String ricevutePecTableName;
    private static String authenticationTable;
    private static int resourceLockedMaxRetryTimes;
    private static long resourceLockedSleepMillis;
    private static String defaultSequenceName;
    private static String gdDocToPublishExtractionQuery;
    private static String gdDocUpdatePubblicato;
    private static String uploadGdDocMongoPath;
    private static String fascicoliTableName;
    private static String gdDocsTableName;
    private static String sottoDocumentiTableName;
    private static String fascicoliGdDocsTableName;
    private static String titoliTableName;
    private static String utentiTableName;
    private static String serviziTableName;
    private static String spedizioniPecTableName;
    private static String indirizziMailTestTableName;
    private static String updateNumberFunctionNameTemplate;
    private static String aggiornamentiParerTableName;
    private static String parametriPubbliciTableName;
//    private static String pubblicazioniAlboTableName;
//    private static String pubblicazioniAlboQueryInsertPubblicazione;
//    private static String pubblicazioniAlboQueryUpdatePubblicazione;

    private static boolean schedulatoreActive = false;

    // parametri pubblici letti dalla tabella dei parametri pubblici tramite il servizio
    private static String serverId;
    private static String mongoRepositoryUri;
    private static String mongoDownloadUri;
    private static String redisHost;
    private static String redisInQueue;
    private static String balboServiceURI;
    private static String maxThreadSpedizioniere;
    private static String getIndeUrlServiceUri;
    private static String spedizioniereApplicazioni;
    private static String fileSupportatiTable;
    private static String schedulatoreConf;
    private static String bdmRestBaseUri;
    private static String systemPecMail;

    public static void initApplicationParams(ServletContext context) throws SQLException, NamingException, ServletException {

        appId = context.getInitParameter("appid");
        appToken = context.getInitParameter("apptoken");
        registriTableName = context.getInitParameter("RegistriTableName");
        spedizioniPecGlobaleTableName = context.getInitParameter("SpedizioniPecGlobaleTableName");
        ricevutePecTableName = context.getInitParameter("RicevutePecTableName");
        resourceLockedMaxRetryTimes = Integer.parseInt(context.getInitParameter("ResourceLockedMaxRetryTimes"));
        resourceLockedSleepMillis = Long.parseLong(context.getInitParameter("ResourceLockedSleepMillis"));     
        defaultSequenceName = context.getInitParameter("DefaultSequenceName");
        gdDocToPublishExtractionQuery = context.getInitParameter("GdDocToPublishExtractionQuery");
        gdDocUpdatePubblicato = context.getInitParameter("GdDocUpdatePubblicato");
        uploadGdDocMongoPath = context.getInitParameter("UploadGdDocMongoPath");
        fascicoliTableName = context.getInitParameter("FascicoliTableName");
        fascicoliGdDocsTableName = context.getInitParameter("FascicoliGdDocsTableName");
        titoliTableName = context.getInitParameter("TitoliTableName");
        utentiTableName = context.getInitParameter("UtentiTableName");
        gdDocsTableName = context.getInitParameter("GdDocsTableName");
        sottoDocumentiTableName = context.getInitParameter("SottoDocumentiTableName");
        spedizioniPecTableName = context.getInitParameter("SpedizioniPecTableName");
        indirizziMailTestTableName = context.getInitParameter("IndirizziMailTestTableName");
        updateNumberFunctionNameTemplate = context.getInitParameter("UpdateNumberFunctionNameTemplate");
        aggiornamentiParerTableName = context.getInitParameter("AggiornamentiParerTableName");
        spedizioniereApplicazioni = context.getInitParameter("SpedizioniereApplicazioni");
        fileSupportatiTable = context.getInitParameter("FileSupportatiTableName");
        parametriPubbliciTableName = context.getInitParameter("ParametersTableName");
        serviziTableName = context.getInitParameter("ServiziTableName");
//        pubblicazioniAlboTableName = context.getInitParameter("PubblicazioniAlboTableName");
//        pubblicazioniAlboQueryInsertPubblicazione = context.getInitParameter("PubblicazioniAlboQueryInsertPubblicazione");
//        pubblicazioniAlboQueryUpdatePubblicazione = context.getInitParameter("PubblicazioniAlboQueryUpdatePubblicazione");
        
        schedulatoreActive = Boolean.parseBoolean(context.getInitParameter("schedulatore.active"));

        readAuthenticationTable(context);

        try {
            initilizeSupporetdFiles(fileSupportatiTable);

            ConfigParams.initConfigParams();
            serverId = ConfigParams.getParam("serverIdentifier");
            mongoRepositoryUri = ConfigParams.getParam("mongoConnectionString");
            mongoDownloadUri = ConfigParams.getParam("mongoDownloadConnectionString");
            redisHost = ConfigParams.getParam("masterChefHost");
            redisInQueue = ConfigParams.getParam("masterChefPushingQueue");

            // balbo service URI in parametri pubblici
            balboServiceURI = ConfigParams.getParam("balboServiceURI");

            getIndeUrlServiceUri = ConfigParams.getParam("getIndeUrlServiceUri");
            schedulatoreConf= ConfigParams.getParam("schedulatoreConfJson");
            bdmRestBaseUri = ConfigParams.getParam("BdmRestBaseUri");
            systemPecMail = ConfigParams.getParam("SystemPecMail");
        }
        catch (Exception ex) {
           log.error("errore nell'inizializzazione: ", ex);
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
    private static void initilizeSupporetdFiles(String fileSupportatiTable) throws SQLException, NamingException {
        
        
        String sqlText = "SELECT * FROM " +  fileSupportatiTable;
        // ottengo una connessione al db
        try (Connection dbConn = UtilityFunctions.getDBConnection();
             PreparedStatement ps = dbConn.prepareStatement(sqlText);) {

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet results = ps.executeQuery();
            List<SupportedFile> supportedFiles = new ArrayList<>();
            while (results.next()) {
                supportedFiles.add(new SupportedFile(results.getString("mime_type"), results.getString("estensione"), results.getBoolean("convertibile_pdf")));
            }
            ApplicationParams.setSupportedFileList(supportedFiles);
        }
    }

    public static Logger getLog() {
        return log;
    }

    public static List<SupportedFile> getSupportedFileList() {
        return supportedFileList;
    }

    public static void setSupportedFileList(List<SupportedFile> supportedFileList) {
        ApplicationParams.supportedFileList = supportedFileList;
    }

    public static String getAppId() {
        return appId;
    }

    public static String getAppToken() {
        return appToken;
    }

    public static String getRegistriTableName() {
        return registriTableName;
    }

    public static String getSpedizioniPecGlobaleTableName() {
        return spedizioniPecGlobaleTableName;
    }

    public static String getRicevutePecTableName() {
        return ricevutePecTableName;
    }

    public static void setRicevutePecTableName(String ricevutePecTableName) {
        ApplicationParams.ricevutePecTableName = ricevutePecTableName;
    }
    
    public static String getAuthenticationTable() {
        return authenticationTable;
    }

    public static int getResourceLockedMaxRetryTimes() {
        return resourceLockedMaxRetryTimes;
    }

    public static long getResourceLockedSleepMillis() {
        return resourceLockedSleepMillis;
    }

    public static String getDefaultSequenceName() {
        return defaultSequenceName;
    }

    public static String getGdDocToPublishExtractionQuery() {
        return gdDocToPublishExtractionQuery;
    }

    public static String getGdDocUpdatePubblicato() {
        return gdDocUpdatePubblicato;
    }

    public static String getUploadGdDocMongoPath() {
        return uploadGdDocMongoPath;
    }

    public static String getFascicoliTableName() {
        return fascicoliTableName;
    }

    public static String getGdDocsTableName() {
        return gdDocsTableName;
    }

    public static String getSottoDocumentiTableName() {
        return sottoDocumentiTableName;
    }

    public static String getFascicoliGdDocsTableName() {
        return fascicoliGdDocsTableName;
    }

    public static String getTitoliTableName() {
        return titoliTableName;
    }

    public static String getUtentiTableName() {
        return utentiTableName;
    }

    public static String getSpedizioniPecTableName() {
        return spedizioniPecTableName;
    }

    public static String getUpdateNumberFunctionNameTemplate() {
        return updateNumberFunctionNameTemplate;
    }

    public static String getAggiornamentiParerTableName() {
        return aggiornamentiParerTableName;
    }

//    public static String getPubblicazioniAlboTableName() {
//        return pubblicazioniAlboTableName;
//    }
//
//    public static String getPubblicazioniAlboQueryInsertPubblicazione() {
//        return pubblicazioniAlboQueryInsertPubblicazione;
//    }
//
//    public static String getPubblicazioniAlboQueryUpdatePubblicazione() {
//        return pubblicazioniAlboQueryUpdatePubblicazione;
//    }

    public static boolean isSchedulatoreActive() {
        return schedulatoreActive;
    }

    public static String getServerId() {
        return serverId;
    }

    public static String getMongoRepositoryUri() {
        return mongoRepositoryUri;
    }

    public static String getMongoDownloadUri() {
        return mongoDownloadUri;
    }

    public static String getRedisHost() {
        return redisHost;
    }

    public static String getRedisInQueue() {
        return redisInQueue;
    }

    public static String getBalboServiceURI() {
        return balboServiceURI;
    }

    public static String getMaxThreadSpedizioniere() {
        return maxThreadSpedizioniere;
    }

    public static String getGetIndeUrlServiceUri() {
        return getIndeUrlServiceUri;
    }

    public static String getBdmRestBaseUri() {
        return bdmRestBaseUri;
    }

    public static void setBdmRestBaseUri(String bdmRestBaseUri) {
        ApplicationParams.bdmRestBaseUri = bdmRestBaseUri;
    }

    public static String getSystemPecMail() {
        return systemPecMail;
    }

    public static void setSystemPecMail(String systemPecMail) {
        ApplicationParams.systemPecMail = systemPecMail;
    }

    public static String getSpedizioniereApplicazioni() {
        return spedizioniereApplicazioni;
    }

    public static String getFileSupportatiTable() {
        return fileSupportatiTable;
    }
    
    public static String  getSchedulatoreConf(){
        return schedulatoreConf;
    }

    public static String getParametriPubbliciTableName() {
        return parametriPubbliciTableName;
    }

    public static void setParametriPubbliciTableName(String parametriPubbliciTableName) {
        ApplicationParams.parametriPubbliciTableName = parametriPubbliciTableName;
    }

    public static String getServiziTableName() {
        return serviziTableName;
    }

    public static void setServiziTableName(String serviziTableName) {
        ApplicationParams.serviziTableName = serviziTableName;
    }

    public static String getIndirizziMailTestTableName() {
        return indirizziMailTestTableName;
    }

    public static void setIndirizziMailTestTableName(String indirizziMailTestTableName) {
        ApplicationParams.indirizziMailTestTableName = indirizziMailTestTableName;
    }

    /**
     * torna un parametro pubblico in base al suo nome
     * NB. cercare di non usare questa funzione a meno che non sia strettamente necessario
     * @param paramName il nome del parametro
     * @return  il valore del parametro identificato dal nome passato
     */
    public static String getOtherPublicParam(String paramName) {
        return ConfigParams.getParam(paramName);
    }
    
    // per i processi bonita
    public static String getSetCurrentActivityFunctionName(ServletContext context, String idApplicazione) {
        return context.getInitParameter(idApplicazione + "SetCurrentActivityFunctionName");
    }

    // per i processi bdm
    public static String getSetCurrentStepFunctionName(ServletContext context, String tipoOggetto) {
        return context.getInitParameter(tipoOggetto + "SetCurrentStepFunctionName");
    }
}
