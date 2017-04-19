/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.jobs.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import javax.naming.NamingException;
import org.apache.commons.io.IOUtils;
import org.quartz.CronExpression;

/**
 *
 * @author andrea
 */
public class Test {

    public static void main(String[] args) throws JsonProcessingException, FileNotFoundException, IOException, SQLException, NamingException, NotAuthorizedException, IodaDocumentException, ParseException {
        
        
                
        System.exit(0);
        
        Date date;
        CronExpression exp;
          
        String timeSettato = "0 10 9 1/1 * ? *";

        exp = new CronExpression(timeSettato);
        date = exp.getNextValidTimeAfter(new Date());
        LocalDateTime nowLocal = LocalDateTime.now();
        
        
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);  
        int oraDB = cal.get(Calendar.HOUR_OF_DAY);
        int minutiDB = cal.get(Calendar.MINUTE);
        
        System.out.println("ora letta da db: " + oraDB);
        System.out.println("minuti letti db: " + minutiDB);
        
        System.out.println("ora attuale: " + nowLocal.getHour());
        System.out.println("minuti attuali: " + nowLocal.getMinute());

        //Date now = Date.from(nowLocal.atZone(ZoneId.systemDefault()).toInstant());
        
        if (nowLocal.getHour() == oraDB){
            if (nowLocal.getMinute() > minutiDB){
                System.out.println("può partire");
            }
            else{
                System.out.println("non può partire");
            }
        }
        else if(nowLocal.getHour() > oraDB){
            System.out.println("può partire");
        }
        else{
            System.out.println("non può partire");
        }

    
        System.exit(0);
        String query = "SELECT attivo " +
                        "FROM bds_tools.spedizioniere_applicazioni " +
                        "WHERE  id_applicazione_spedizioniere = ? ;";              
        try (
                
                Connection connection = DriverManager.getConnection("jdbc:postgresql://gdml:5432/argo","argo", "siamofreschi");
           
                //Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = connection.prepareStatement(query);
            )  {
            String idApplicazione = "procton";
            ps.setString(1, idApplicazione);
            ResultSet re = ps.executeQuery();
            re.next();
            System.out.println( re.getBoolean("attivo"));
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.out.println("Eccezione!");
        }
        
        
        System.exit(0);
        
        JobParams jp = new JobParams();
        jp.addParam("k1", "v1");
        jp.addParam("k2", "v2");
        JobDescriptor jd = new JobDescriptor();
        jd.setJobParams(jp);
        jd.setSchedule("schedule1");
        jd.setName("job name");
        jd.setClassz("job class");
        JobList jl = new JobList();
        jl.addJob(jd);

        ObjectMapper mapper = new ObjectMapper();
        String writeValueAsString = mapper.writeValueAsString(jl);
        FileOutputStream out = new FileOutputStream("/tmp/joblist.json");
        out.write(writeValueAsString.getBytes("UTF8"));
        out.close();
        System.out.println(writeValueAsString);
        String json = IOUtils.toString(new FileReader("/tmp/joblist.json"));
        JobList jl2 = null;
        jl2 = mapper.readValue(json, JobList.class);
        //jl2 = mapper.readValue("/tmp/joblist.json", JobList.class);
        System.out.println(jl2.getJobs().get(0).getJobParams().getParam("k1"));

    }
    
    
    private static GdDoc getGdDocById(String idGdDoc) throws SQLException, NamingException, NotAuthorizedException, IodaDocumentException{
        
        
        
        GdDoc gdDoc = null;
        
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
        ) {
            String prefix = "babel_suite_";//UtilityFunctions.checkAuthentication(dbConnection, getIdApplicazione(), getTokenApplicazione());
            
            HashMap<String, Object> additionalData = new HashMap <String, Object>(); 
            additionalData.put("FASCICOLAZIONI", "LOAD");
            additionalData.put("PUBBLICAZIONI", "LOAD");
            
            gdDoc = IodaDocumentUtilities.getGdDocById(dbConnection, idGdDoc, additionalData, prefix);
            gdDoc.setId(idGdDoc);
        }
        return gdDoc;    
    }
    
}
