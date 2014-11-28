/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools;

import it.bologna.ausl.bds_tools.jobs.PulitoreDownloadMongo;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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

    static Scheduler sched;

    @Override
    public void init() throws ServletException {
        super.init();
        StdSchedulerFactory sf = new StdSchedulerFactory();
        Properties prop = new Properties();
        prop.put("org.quartz.scheduler.instanceName", "BabelScheduler");
        prop.put("org.quartz.threadPool.threadCount", "3");
        prop.put("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
        System.out.println("Mongoschedulo");
        try {
            sf.initialize(prop);
            sched = sf.getScheduler();

            JobDetail job = newJob(PulitoreDownloadMongo.class).withIdentity("PulitoreDownloadMongo", "group1")
                    .usingJobData("interval", 24).usingJobData("connectUri", "mongodb://argo:siamofreschi@procton3/downloadgdml")
                    .build();

            Trigger trigger = newTrigger().withIdentity("trigger1", "group1").startNow()
                    .withSchedule(simpleSchedule()
                            .withIntervalInSeconds(10)
                            .repeatForever())
                    .build();
            sched.scheduleJob(job, trigger);
            sched.start();
        } catch (SchedulerException ex) {
            Logger.getLogger(Schedulatore.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("Mongoschedulato !");

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
        PrintWriter out = response.getWriter();
        try {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet Schedulatore</title>");
            out.println("</head>");
            out.println("<body><table>");
            try {
                Set<JobKey> jobKeys = sched.getJobKeys(GroupMatcher.anyJobGroup());
                for (JobKey jk : jobKeys) {
                    JobDetail jobDetail = sched.getJobDetail(jk);
                    out.println("<tr><td>" + jobDetail + "</td></tr>");

                }
            } catch (SchedulerException sk) {
                out.println(sk);
            }
            out.println("</table><h1>Servlet Schedulatore at " + request.getContextPath() + "</h1>");
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
            Logger.getLogger(Schedulatore.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
