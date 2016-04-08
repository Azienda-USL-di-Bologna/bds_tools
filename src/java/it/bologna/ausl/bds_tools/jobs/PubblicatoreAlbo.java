package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.balboclient.BalboClient;
import it.bologna.ausl.balboclient.classes.AllegatoPubblicazione;
import it.bologna.ausl.balboclient.classes.Pubblicazione;
import it.bologna.ausl.balboclient.classes.PubblicazioneAlbo;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.Registro;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.PubblicazioneIoda;
import it.bologna.ausl.ioda.iodaobjectlibrary.SottoDocumento;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
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
public class PubblicatoreAlbo implements Job {

    private static final Logger log = LogManager.getLogger(PubblicatoreAlbo.class);
    private static final String TIPO_PROVVEDIMENTO_NON_RILEVANTE = "non_rilevante";
    private static final String PUBBLICATORE = "balbo";

    //private int intervalDays;
//    public PubblicatoreAlbo() {
//        this.intervalDays = 30;
//    }
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.debug("Pubblicatore Albo Started");

        Connection dbConn = null;
        PreparedStatement ps = null;
        try {
            dbConn = UtilityFunctions.getDBConnection();
            log.debug("connesisone db ottenuta");

            dbConn.setAutoCommit(false);
            log.debug("inizio transazione, autoCommit settato a: " + dbConn.getAutoCommit());

            log.debug("setto balboClient...");
            BalboClient balboClient = new BalboClient(ApplicationParams.getBalboServiceURI());
            log.debug("balboClient settato");

            //Qui dentro metto tutti i regsitri man mano che scorro i gddoc in modo da evitare di andare a leggere lo stesso dato più volte sul DB
            HashMap<String, Registro> mappaRegistri = new HashMap<>();

            ArrayList<GdDoc> listaGdDocsDaPubblicare = IodaDocumentUtilities.getGdDocsDaPubblicare(dbConn);
            Registro registroGddoc;

            for (GdDoc gddoc : listaGdDocsDaPubblicare) {

                if(mappaRegistri.containsKey(gddoc.getCodiceRegistro()))
                {
                    //se è già presente questo codice registro, allora utilizzo questo
                    registroGddoc = mappaRegistri.get(gddoc.getCodiceRegistro());
                }    
                else
                {
                    //Altrimenti devo calcolarla e poi la inserisco nella mappa
                    Registro reg = Registro.getRegistro(gddoc.getCodiceRegistro(), dbConn);
                    registroGddoc = reg;
                    mappaRegistri.put(reg.codiceRegistro, reg);
                }

                //Per ogni Gddoc recupero i sotto documenti da assegnare  alla pubblicazione
                log.debug("caricamento sottoDocumenti...");
                List<SottoDocumento> sottodocumenti = IodaDocumentUtilities.caricaSottoDocumenti(dbConn, gddoc.getId());
                log.debug("sottoDocumenti:");
                sottodocumenti.stream().forEach(s -> {log.debug(s.toString());});

                //Per ogni GDDOC mi recupero le pubblicazioni associate. Sono solo quelle da pubblicare, opportunamente filtrate quando recuperiamo la lista dei Gddoc da pubblicare
                List<PubblicazioneIoda> pubblicazioniGdDoc = gddoc.getPubblicazioni();
                log.debug("pubblicazioni:");
                pubblicazioniGdDoc.stream().forEach(p -> {log.debug(p.toString());});
                for(PubblicazioneIoda pubbIoda : pubblicazioniGdDoc){
                    log.debug("pubblicazione: " + pubbIoda.toString());

                    log.debug("crezione pubblicazione per balbo...");
                    PubblicazioneAlbo datiAlbo = new PubblicazioneAlbo(pubbIoda.getDataDal().toDate(),pubbIoda.getDataAl().toDate());

                    Pubblicazione pubblicazione = new Pubblicazione(
                            gddoc.getAnnoRegistrazione(),  
                            gddoc.getNumeroRegistrazione(), 
                            registroGddoc.codiceRegistro, 
                            registroGddoc.descPubbAlbo, 
                            pubbIoda.getEsecutivita(), 
                            gddoc.getNomeStrutturaFirmatario(), 
                            UtilityFunctions.fixXhtml(gddoc.getOggetto()), 
                            gddoc.getTipoOggettoOrigine(), 
                            "attivo", 
                            gddoc.getData().toDate(), 
                            gddoc.getDataRegistrazione().toDate(),
                            gddoc.getDataRegistrazione().toDate(), 
                            datiAlbo, 
                            null);
    
                    //ad ogni pubblicazione, assegno i sottodocumenti del gddoc a cui appartengono
                    for (SottoDocumento sottodoc : sottodocumenti) {
                        log.debug("aggiunta allegato dal sottodocumento: " + sottodoc.toString());
                        AllegatoPubblicazione allegatoPubblicazione = new AllegatoPubblicazione(sottodoc.getNome(), sottodoc.getCodiceSottoDocumento());
                        pubblicazione.addAllegato(allegatoPubblicazione);
                    }
                    log.debug("pubblicazione balbo: " + pubblicazione.toString());
                    
                    Pubblicazione p = null;
                    try {
                        log.info("pubblicazione effettiva su balbo...");
                        p = balboClient.pubblica(pubblicazione);
                    }
                    catch (org.springframework.web.client.HttpClientErrorException ex) {
                        log.error("errore nella pubblicazione ", ex.getResponseBodyAsString());
                        throw ex;
                    }
                    log.info("pubblicato, pubblicazione balbo aggiornata: " + p.toString());

                    pubbIoda.setAnnoPubblicazione(p.getAnnoPubblicazione());
                    pubbIoda.setNumeroPubblicazione(p.getNumeroPubblicazione());
                    pubbIoda.setPubblicatore(PUBBLICATORE);

                    log.info("update pubblicazione ioda locale...");
                    IodaDocumentUtilities.UpdatePubblicazione(pubbIoda);
                    log.info("update pubblicazione ioda locale completato");
                }
            } //Fine ciclo FOR GDDOC
            
        } //try
        catch (Throwable t) {
            log.fatal("Errore nel pubblicatore albo", t);
            try {
                if (dbConn != null) {
                    log.info("rollback...");
                    dbConn.rollback();
                }
            }
            catch (Exception ex) {
                log.error("errore nel rollback", ex);
            }
            throw new JobExecutionException(t);
        }
        finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception ex) {
                    log.error(ex);
                }
            }
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (Exception ex) {
                    log.error(ex);
                }
            }
            log.debug("Pubblicatore Albo Ended");
        }
    }
}
