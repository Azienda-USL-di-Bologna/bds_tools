package it.bologna.ausl.bds_tools.jobs;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import it.bologna.ausl.mongowrapper.MongoWrapper;
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
        MongoWrapper mw = null;
        try {
            mw = new MongoWrapper(connectUri);            
            Calendar targetDate = Calendar.getInstance();
            targetDate.add(Calendar.DATE, -intervalDays);
//            targetDate.add(Calendar.HOUR, -intervalDays);
            List<String> uuidsToDelete = mw.getDeletedLessThan(targetDate.getTimeInMillis());
            for (String uuid : uuidsToDelete) {
                mw.erase(uuid);
            }
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

    public void setInterval(int hour) {
        this.intervalDays = hour;
    }

    public void setInterval(String hour) {
        this.intervalDays = Integer.valueOf(hour);
    }

}
