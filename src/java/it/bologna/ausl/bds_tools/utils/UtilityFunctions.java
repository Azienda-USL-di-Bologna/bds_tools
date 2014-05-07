package it.bologna.ausl.bds_tools.utils;

/**
 *
 * @author Giuseppe De Marco (gdm)
 */
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class UtilityFunctions {
private static Logger log = Logger.getLogger(UtilityFunctions.class);    

    public static Connection getDBConnection() throws SQLException, NamingException {
        Context initContext = new InitialContext();
        Context envContext  = (Context)initContext.lookup("java:/comp/env");
        DataSource ds = (DataSource)envContext.lookup("jdbc/argo");
        Connection conn = ds.getConnection();
        return conn;
    }
    
    public static boolean checkAuthentication(Connection dbConn, String authenticationTable, String idApplicazione, String token) {
    PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource("it/bologna/ausl/bds_tools/conf/log4j.properties")); 
    // configuro il logger per la console
    BasicConfigurator.configure(); 
        try {
            String sqlText = "SELECT id_applicazione" +
                            " FROM " + authenticationTable +
                            " WHERE id_applicazione = ? and token = ?";

            PreparedStatement ps = dbConn.prepareStatement(sqlText);
            ps.setString(1, idApplicazione);
            ps.setString(2, token);
            String query = ps.toString().substring(0, ps.toString().lastIndexOf("=") + 1) + " ******";
            log.debug("eseguo la query: " + query + " ...");
            dbConn.setAutoCommit(true);
            ResultSet authenticationResultSet = null;
                authenticationResultSet = ps.executeQuery();

            if (authenticationResultSet.next() == false) {
                String message = "applicazione: " + idApplicazione + " non autorizzata";
                log.error(message);
                return false;
            }
            else {
                String message = "applicazione: " + idApplicazione + " autorizzata";
                log.info(message);
                return true;
            }
        }
        catch (Exception ex) {
            log.error("Errore", ex);
            return false;
        }
    }
    
    public static String arrayToString(Object[] array, String separator) {
        StringBuilder builder = new StringBuilder();
        if (array == null || array.length == 0)
            return null;
        builder.append(array[0].toString());
        for (int i=1; i<array.length; i++)
            builder.append(separator).append(array[i].toString());
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
}
