package it.bologna.ausl.bds_tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.bologna.ausl.bds_tools.jobs.utils.JobDescriptor;
import it.bologna.ausl.bds_tools.jobs.utils.JobList;
import it.bologna.ausl.bds_tools.jobs.utils.JobParams;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Properties;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import static org.quartz.JobBuilder.newJob;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import org.quartz.Trigger;
import static org.quartz.TriggerBuilder.newTrigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

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

        String activeString = getServletContext().getInitParameter("schedulatore.active");
        if (activeString != null) {
            active = Boolean.parseBoolean(activeString);
        }

        String configFile = getServletContext().getInitParameter("schedulatore.configfile");
        if (configFile != null) {
            CONF_FILE_NAME = configFile;
        }
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
        } catch (SchedulerException ex) {
            log.fatal(ex);
        } catch (IOException ex) {
            log.fatal(ex);
        } catch (ClassNotFoundException ex) {
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

            URL jobConfURL = Thread.currentThread().getContextClassLoader().getResource(this.getClass().getPackage().getName().replace(".", "/") + "/" + CONF_PACKAGE_SUFFIX + "/" + CONF_FILE_NAME);
            if (jobConfURL == null) {
                throw new FileNotFoundException("Schedulatore configuration file not found");
            }
            String jobString = IOUtils.toString(jobConfURL);
            ObjectMapper mapper = new ObjectMapper();
            JobList jobList = mapper.readValue(jobString, JobList.class
            );
            for (JobDescriptor jd
                    : jobList.getJobs()) {
                JobParams jp = jd.getJobParams();
                JobBuilder jb = newJob((Class< ? extends Job>) Class.forName(this.getClass().getPackage().getName() + "." + JOB_PACKAGE_SUFFIX + "." + jd.getClassz())).withIdentity(jd.getName(), "group1");
                for (String k : jp.getParamNames()) {
                    jb.usingJobData(k, jp.getParam(k));
                }
                JobDetail job = jb.build();
                Trigger trigger = newTrigger().withIdentity("trigger_" + jd.getName(), "group1").startNow()
                        .withSchedule(simpleSchedule()
                                .withIntervalInSeconds(Integer.valueOf(jd.getSchedule()))
                                .repeatForever())
                        .build();
                sched.scheduleJob(job, trigger);
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
        if (request.getParameter("reload") != null) {
            try {
                quarzInit();
            } catch (SchedulerException ex) {
                log.fatal(ex);
            } catch (ClassNotFoundException ex) {
                log.fatal(ex);
                throw new ServletException("Errore ricaricando configurazione", ex);
            }
        }
        if (request.getParameter("start") != null) {
            try {
                active = true;
                quarzInit();
            } catch (SchedulerException ex) {
                log.fatal(ex);
            } catch (ClassNotFoundException ex) {
                log.fatal(ex);
                throw new ServletException("Errore ricaricando configurazione", ex);
            }
        }
        if (request.getParameter("stop") != null) {
            try {
                active = false;
                quarzStop();
            } catch (SchedulerException ex) {
                log.fatal(ex);
            }
        }
        PrintWriter out = response.getWriter();
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
