package it.bologna.ausl.bds_tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.bologna.ausl.bds_tools.jobs.utils.JobDescriptor;
import it.bologna.ausl.bds_tools.jobs.utils.JobList;
import it.bologna.ausl.bds_tools.jobs.utils.JobParams;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.ConfigParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import static org.quartz.TriggerBuilder.newTrigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import static org.quartz.JobBuilder.newJob;

/**
 *
 * @author andrea
 */
public class Schedulatore extends HttpServlet {

    private static final Logger log = LogManager.getLogger(Schedulatore.class);
    private static Scheduler sched;
    private static final String JOB_PACKAGE_SUFFIX = "jobs";
    private static final String CONF_PACKAGE_SUFFIX = "conf";
    private static String CONF_FILE_NAME = "schedulatore_conf.json";
    private static boolean active = false;

    @Override
    public void init() throws ServletException {
        super.init();
        log.info("Inizio avvio Schedulatore");

        active = ApplicationParams.isSchedulatoreActive();

        // questo va cambiato con il parsing del json letto dai parametri pubblici
       // String configFile = getServletContext().getInitParameter("schedulatore.configfile");
       // if (configFile != null) {
//            CONF_FILE_NAME = configFile;
//        }
        try {
            StdSchedulerFactory sf = new StdSchedulerFactory();
            Properties prop = new Properties();
            prop.put("org.quartz.scheduler.instanceName", "BabelScheduler");
            prop.put("org.quartz.threadPool.threadCount", "3");
            prop.put("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
            System.out.println("Schedulo");

            sf.initialize(prop);
            sched = sf.getScheduler();
            quarzInit();
        } catch (SchedulerException | IOException | ClassNotFoundException ex) {
            log.fatal(ex);
        }

        log.info("Termine avvio Schedulatore");
    }

    private void quarzStop() throws SchedulerException {
        if (sched.isStarted()) {
            sched.standby();
            sched.clear();
        }
    }

    private void quarzInit() throws SchedulerException, IOException, ClassNotFoundException {
        if (active) {
            log.info("Inizializzazione schedulatore");
            quarzStop();
            
            // Ricarico i parametri del db
            ConfigParams.initConfigParams();

           // URL jobConfURL = Thread.currentThread().getContextClassLoader().getResource(this.getClass().getPackage().getName().replace(".", "/") + "/" + CONF_PACKAGE_SUFFIX + "/" + CONF_FILE_NAME);
           // if (jobConfURL == null) {
//                throw new FileNotFoundException("Schedulatore configuration file not found");
//            }
        
            String jobString = ApplicationParams.getSchedulatoreConf();
            ObjectMapper mapper = new ObjectMapper();
            JobList jobList = mapper.readValue(jobString, JobList.class);
            for (JobDescriptor jd : jobList.getJobs()) {
                JobParams jp = jd.getJobParams();
                JobBuilder jb = newJob((Class< ? extends Job>) Class.forName(this.getClass().getPackage().getName() + "." + JOB_PACKAGE_SUFFIX + "." + jd.getClassz())).withIdentity(jd.getName(), "group1");
                for (String k : jp.getParamNames()) {
                    jb.usingJobData(k, jp.getParam(k));
                }
                JobDetail job = jb.build();
                Trigger trigger = null;
                
                if (jd.getActive()){
                    // controllo se ha regole cron
                    if (jd.getRole()!= null && !jd.getRole().equals("")){
                        trigger = TriggerBuilder
                                    .newTrigger()
                                    .withIdentity("dummyTriggerName", "group1")
                                    .withSchedule(
                                    // regola cron
                                    CronScheduleBuilder.cronSchedule(jd.getRole()))
                                    .build();
                    }
                    else{
                        trigger = newTrigger().withIdentity("trigger_" + jd.getName(), "group1").startNow()
                                    .withSchedule(simpleSchedule()
                                            .withIntervalInSeconds(Integer.valueOf(jd.getSchedule()))
                                            .repeatForever())
                                    .build();
                    } 
                    sched.scheduleJob(job, trigger);
                }
            }
            sched.start();
        }
    }

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        //response.addHeader("Access-Control-Allow-Origin", "*");
        
        String command = request.getParameter("schedulatore");
        PrintWriter out = response.getWriter();
        
        //String esito =  "inizio processRequest";
        
        //log.debug("inizio processRequest");
        
        //log.debug("valore_command: " + command);
        //esito += "valore_command: " + command;
        
        if (command != null && !command.equals("")) {
            switch (command){
                case "reload":
                    //esito += " reload";
                    log.debug("case reload");
                    try {
                        quarzInit();
                    } catch (SchedulerException ex) {
                        log.fatal(ex);
                    } catch (ClassNotFoundException ex) {
                        log.fatal(ex);
                        throw new ServletException("Errore ricaricando configurazione", ex);
                    }
                    break;
                case "start":
                    //esito += " start";
                    log.debug("case start");
                    try {
                        active = true;
                        quarzInit();
                    } catch (SchedulerException ex) {
                        log.fatal(ex);
                    } catch (ClassNotFoundException ex) {
                        log.fatal(ex);
                        throw new ServletException("Errore ricaricando configurazione", ex);
                    }
                    break;
                case "stop":
                    //esito += " stop";
                    log.debug("case stop");
                    try {
                        active = false;
                        quarzStop();
                    } catch (SchedulerException ex) {
                        log.fatal(ex);
                    }
                    break;
                case "json":
                    //esito += " json";
                    log.debug("case json");
                    response.setContentType("application/json");
                    String json = ApplicationParams.getSchedulatoreConf();
                    out.println(json);
                    out.close();
                    break;
                case "status":
                    //esito += " status";
                    log.debug("case status");
                    response.setContentType("application/json");
                    String res = "{\"active\":" + active + "}";
                    out.println(res);
                    out.close();
                    break;
                case "fireService":
                    //esito += "fireService";
                    log.debug("case fireService");
                    String serviceName = request.getParameter("service");
                    String content = request.getParameter("content"); // contiene i guid
                    log.debug("serviceName: " + serviceName);
                    log.debug("content: " + content);
                    if (serviceName != null && !serviceName.equals("") && content != null && !content.equals("")) {
                        try {
                            //esito += " pre executeNow";
                            executeNow(serviceName, content);
                            //esito += " post executeNow";
                        } catch (SchedulerException ex) {
                            log.debug("Eccezione nel lanciare il servizio!" + ex);
                        }
                    }
                    break;
                default:
                    log.debug("case default");
                    //esito += " default";
                    try {
                        /* TODO output your page here. You may use following sample code. */
                        out.println("<!DOCTYPE html>");
                        out.println("<html>");
                        out.println("<head>");
                        out.println("<title>Servlet Schedulatore</title>");
                        out.println("</head>");
                        out.println("<body>");
                        if (active) {
                            out.println("<table border='1'>");
                            try {
                                Set<JobKey> jobKeys = sched.getJobKeys(GroupMatcher.anyJobGroup());
                                for (JobKey jk : jobKeys) {
                                    JobDetail jobDetail = sched.getJobDetail(jk);
                                    out.println("<tr><td>" + jobDetail + "</td>");
                                    for (Trigger t : sched.getTriggersOfJob(jk)) {
                                        out.println("<td>" + t.getDescription() + " [" + t.getStartTime().toString() + "] " + t.getPreviousFireTime().toString() + " - " + t.getNextFireTime().toString() + "</td>");
                                    }
                                    out.println("</tr>");
                                }
                            } catch (SchedulerException sk) {
                                out.println(sk);
                            }
                            out.println("</table><h1>Servlet Schedulatore at " + request.getContextPath() + "</h1>");
                            out.println("<form><input type=\"hidden\" name=\"reload\" value=\"reload\"/><input type=\"submit\" value=\"Ricarica Configurazione\" /></form>");
                        } else {
                            out.println("<h1>Schedulatore non attivo</h1>");
                        }
                        out.println("</body>");
                        out.println("</html>");
                    } finally {
                        out.close();
                    }
                    break;          
            }
        } 
        
        
        
        String service = request.getParameter("service");
        log.debug("SERVICE: " + service);
        if (service != null && !service.equals("")) {
            String json = request.getParameter("json");
            log.debug("JSON: " + json);
            if (json != null && !json.equals("")) {
                String status = request.getParameter("status");
                log.debug("STATUS: " + status);
                if (status != null && !status.equals("")) {
                    String queryUpdateStatus =  "UPDATE " + ApplicationParams.getParametriPubbliciTableName() + " " +
                                                "SET val_parametro=? " +
                                                "WHERE nome_parametro=?;";
                    try (
                        Connection dbConnection = UtilityFunctions.getDBConnection();
                        PreparedStatement ps = dbConnection.prepareStatement(queryUpdateStatus)
                    ) {
                        int i = 1;
                        ps.setString(i++, json);
                        ps.setString(i++, "schedulatoreConfJson");
                        log.debug("QUERY: " + ps);
                        ps.executeUpdate();
                        ConfigParams.initConfigParams();
                    } catch (SQLException | NamingException ex) {
                        log.error("Eccezione nell'update dello status del servizio " + service + " in " + status, ex);
                    }
                }
            }
        }
        //out.println(esito);
    }
    
    private String getJsonFromDb(){
        String queryUpdateStatus =  "SELECT val_parametro " + 
                                    "FROM " + ApplicationParams.getParametriPubbliciTableName() + " " +
                                    "WHERE nome_parametro=?;";
        try (
            Connection dbConnection = UtilityFunctions.getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(queryUpdateStatus)
        ) {
            int i = 1;
            ps.setString(i++, "schedulatoreConfJson");
            log.debug("QUERY: " + ps);
            ResultSet res = ps.executeQuery();
            while (res.next()) {                
                return res.getString("val_parametro");
            }
        } catch (SQLException | NamingException ex) {
            log.error("Eccezione nel caricamento del schedulatoreConfJson");
        }
        return null;
    }
    
    //lancio a volere un servizio
    public void executeNow(String jobName, String content) throws SchedulerException {

        try {
            // Ricarico i parametri del db
            log.info("executeNow: inizio funzione");
            ConfigParams.initConfigParams();
            log.debug("executeNow: lettura parametri json");
            String jobString = ApplicationParams.getSchedulatoreConf();
            ObjectMapper mapper = new ObjectMapper();
            JobList jobList = mapper.readValue(jobString, JobList.class);
            for (JobDescriptor jd : jobList.getJobs()) {
                if (jd.getName().equals(jobName)){
                    if (jd.getActive()){
                        JobParams jp = jd.getJobParams();
                        JobKey jobKey = new JobKey(jobName);
                        JobBuilder jobBuilder =  JobBuilder.newJob((Class< ? extends Job>)
                                Class.forName(this.getClass().getPackage().getName() + "." + JOB_PACKAGE_SUFFIX + "." + jd.getClassz()));

                        for (String k : jp.getParamNames()) {
                            jobBuilder.usingJobData(k, jp.getParam(k));
                        }
                        // questo job non ha trigger ed è lanciato solo a piacimento
                        JobDetail jobDetail = jobBuilder.withIdentity(jobKey).storeDurably().build();

                        log.debug("executeNow: settaggio dati sulla mappa da passare al servizio");
                        
                        JobDataMap jobDataMap = jobDetail.getJobDataMap();
                        jobDataMap.put(jobName, content);
                        //registrazione del job allo schedulatore
                        sched.addJob(jobDetail, true);
                        //lancia subito il servizio
                        log.debug("executeNow: lancio del servizio");
                        sched.triggerJob(jobKey, jobDetail.getJobDataMap());  
                    }
                    else{
                        log.debug("serivizio non attivo");
                    }
                }
            }
        } catch (IOException | ClassNotFoundException ex) {
            log.error(ex);
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
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
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
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Schedulatore Babel";
    }// </editor-fold>

    @Override
    public void destroy() {
        super.destroy(); //To change body of generated methods, choose Tools | Templates.
        try {
            sched.shutdown();

        } catch (SchedulerException ex) {
            log.fatal(ex);
        }
    }

}
