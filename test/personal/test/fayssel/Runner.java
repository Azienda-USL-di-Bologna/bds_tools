/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package personal.test.fayssel;

import it.bologna.ausl.bdm.utilities.DbConnectionPoolManager;
import it.bologna.ausl.bds_tools.jobs.CreatoreFascicoloSpeciale;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.tomcat.jdbc.pool.DataSource;

/**
 *
 * @author fayssel
 */
public class Runner {

    /**
     * @param args the command line arguments
     */
    private static String uriDb;
    private static DbConnectionPoolManager dbConnManager;
    private static org.apache.tomcat.jdbc.pool.DataSource dataSource;
    
    public static void main(String[] args) throws SQLException, IOException, URISyntaxException {
        // TODO code application logic here
        getDbConnection();
        buildUriDb("gdml", 5432, "argo", "postgres", "siamofreschi");
        Connection connection = dataSource.getConnection();
//        CreatoreFascicoloSpeciale.setVicario("FascicoloSpecial2017", "s.fayssel", connection);
    }
    
    public static void buildUriDb(String hostDb, int portaDb, String nomeDb, String username, String password){
        Runner.uriDb = "jdbc:postgresql://" + hostDb + ":" + portaDb + "/" + nomeDb + "?user=" + username + "&password=" + password;
    }
    
    private static void getDbConnection() throws IOException, URISyntaxException{
        dbConnManager = new DbConnectionPoolManager();
        System.out.println("Ottenimento connessione dal db...");
        dataSource = (DataSource) dbConnManager.getConnection(uriDb);
        System.out.println("Connessione ottenuta!");
    }
    
}
