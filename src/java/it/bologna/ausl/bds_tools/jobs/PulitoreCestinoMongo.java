package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */
public class PulitoreCestinoMongo implements Job {

    private static final Logger log = LogManager.getLogger(PulitoreCestinoMongo.class);

    private String connectUri;
    private int intervalDays;

    public PulitoreCestinoMongo() {
        this.intervalDays = 30;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.debug("Pulitore Cestino di mongo Started");
        MongoWrapper mw;
        try {
            mw = new MongoWrapper(connectUri);            
            Calendar targetDate = Calendar.getInstance();
            targetDate.add(Calendar.DAY_OF_MONTH, -intervalDays);
//            if (Calendar.getInstance().getTimeInMillis() - targetDate.getTimeInMillis() < (1000*3600*24)) {
//                throw new JobExecutionException("intervallo troppo breve, minore di 24 ore: " + intervalDays);
//            }
//            targetDate.add(Calendar.HOUR, -intervalDays);
            log.debug("intervalDays: " + intervalDays);
            DateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
            log.debug("reading deleted file less than " + df.format(targetDate.getTime()));
            log.debug("millis: " + targetDate.getTimeInMillis());
            List<String> uuidsToDelete = mw.getDeletedLessThan(targetDate.getTimeInMillis());
            uuidsToDelete.stream().forEach((uuid) -> {
                log.debug("ereasing: " + uuid);
                mw.erase(uuid);
            });
        } catch (Throwable t) {
            log.fatal("Errore nel pulitore Cestino di Mongo", t);
            throw new JobExecutionException(t);
        } finally {

            log.debug("Pulitore Cestino di mongo Ended");
        }
    }

    public void setConnectUri(String uri) {
        this.connectUri = uri;
    }

    public void setIntervalDays(int hour) {
        this.intervalDays = hour;
    }

    public void setIntervalDays(String hour) {
        this.intervalDays = Integer.valueOf(hour);
    }

}
