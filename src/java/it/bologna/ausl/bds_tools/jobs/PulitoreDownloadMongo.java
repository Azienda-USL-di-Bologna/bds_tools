package it.bologna.ausl.bds_tools.jobs;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.util.Calendar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */
public class PulitoreDownloadMongo implements Job {

    private static final Logger log = LogManager.getLogger(PulitoreDownloadMongo.class);

    private String connectUri;
    private int intervalHour;

    public PulitoreDownloadMongo() {
        this.intervalHour = 24;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.debug("Pulitore mongo Started");
        MongoWrapper mw;
        
//        if(true)
//            return;
        
        try {
            mw = new MongoWrapper(connectUri);
            //Trovo i file piu' vecchi di 24 h
            DB db = mw.getDB();
            DBCollection files = db.getCollection("fs.files");

            Calendar c = Calendar.getInstance();
            c.add(Calendar.HOUR_OF_DAY, - 1 * intervalHour);

            DBObject filter = new BasicDBObject("uploadDate", new BasicDBObject("$lt", c.getTime()));

            log.debug(filter);
            //results = files.find();
            try (DBCursor results = files.find(filter)) {
                while (results.hasNext()) {
                    DBObject next = results.next();
                    mw.erase(next.get("_id").toString());
                }
            }
        } catch (Throwable t) {
            log.fatal("Errore nel pulitore Mongo", t);
            throw new JobExecutionException(t);
        } finally {

            log.debug("Pulitore mongo Ended");
        }
    }

    public void setConnectUri(String uri) {
        this.connectUri = uri;
    }

    public void setInterval(int hour) {
        this.intervalHour = hour;
    }

    public void setInterval(String hour) {
        this.intervalHour = Integer.valueOf(hour);
    }

}
