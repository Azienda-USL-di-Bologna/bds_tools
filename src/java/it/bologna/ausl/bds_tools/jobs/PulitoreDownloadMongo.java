package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author gdm
 */
@DisallowConcurrentExecution
public class PulitoreDownloadMongo implements Job {

    private static final Logger log = LogManager.getLogger(PulitoreDownloadMongo.class);

    private String connectUri;
    private int intervalHour;

    public PulitoreDownloadMongo() {
        this.intervalHour = 24;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("Pulitore mongo Started");
        MongoWrapper mw;

//        if(true)
//            return;
        try {
            boolean useMinIO = ApplicationParams.getUseMinIO();
            JSONObject minIOConfig = ApplicationParams.getMinIOConfig();
            String codiceAzienda = ApplicationParams.getCodiceAzienda();
            Integer maxPoolSize = Integer.parseInt(minIOConfig.get("maxPoolSize").toString());
            mw = MongoWrapper.getWrapper(
                    useMinIO,
                    connectUri,
                    minIOConfig.get("DBDriver").toString(),
                    minIOConfig.get("DBUrl").toString(),
                    minIOConfig.get("DBUsername").toString(),
                    minIOConfig.get("DBPassword").toString(),
                    codiceAzienda + "t",
                    maxPoolSize,
                    null);
            log.info("controllo che il repository al quale sono connesso sia temporaneo...");
            if (mw.isTemp()) {
                log.info("sono nel repository temporaneo, vado avanti con la cancellazione...");

                //Trovo i file piu' vecchi di 24 h
                ZonedDateTime nowMinusInterval = ZonedDateTime.now().minusHours(intervalHour);
                List<String> filesToDelete = mw.getFilesLessThan(nowMinusInterval);
                if (filesToDelete != null) {
                    for (String fileId : filesToDelete) {
                        log.info("erasing " + fileId + "...");
                        mw.erase(fileId);
                    }
                }
            } else {
                log.error("NON sono nel repository temporaneo, non faccio niente");
            }

        } catch (Throwable t) {
            log.fatal("Errore nel pulitore Mongo", t);
            throw new JobExecutionException(t);
        } finally {
            log.info("Pulitore mongo Ended");
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
