package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.balboclient.BalboClient;
import it.bologna.ausl.balboclient.classes.AllegatoPubblicazione;
import it.bologna.ausl.balboclient.classes.Pubblicazione;
import it.bologna.ausl.balboclient.classes.PubblicazioneAlbo;
import it.bologna.ausl.balboclient.classes.PubblicazioneCommittente;
import it.bologna.ausl.balboclient.classes.PubblicazioneTrasparenza;
import it.bologna.ausl.bds_tools.exceptions.PubblicatoreException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.Registro;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.DatiProfiloCommittente;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.PubblicazioneIoda;
import it.bologna.ausl.ioda.iodaobjectlibrary.SottoDocumento;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */
@DisallowConcurrentExecution
public class PubblicatoreAlbo implements Job {

    private static final Logger log = LogManager.getLogger(PubblicatoreAlbo.class);
    private static final String TIPO_PROVVEDIMENTO_NON_RILEVANTE = "non_rilevante";
    private static final String PUBBLICATORE = "balbo";
    private static final BalboClient balboClient = new BalboClient(ApplicationParams.getBalboServiceURI());
//    private static final BalboClient balboClient = new BalboClient("http://localhost:10000/");

    //private int intervalDays;
//    public PubblicatoreAlbo() {
//        this.intervalDays = 30;
//    }
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.debug("Pubblicatore Albo Started");

//        if(true)
//            return;
        
        Connection dbConn = null;
        PreparedStatement ps = null;
        try {
            dbConn = UtilityFunctions.getDBConnection();
            log.debug("connessione db ottenuta");

            dbConn.setAutoCommit(false);
            log.debug("autoCommit settato a: " + dbConn.getAutoCommit());

            // qui dentro metto tutti i registri man mano che scorro i gddoc in modo da evitare di andare a leggere lo stesso dato più volte sul DB
            Map<String, Registro> mappaRegistri = new HashMap<>();

            List<GdDoc> listaGdDocsDaPubblicare = IodaDocumentUtilities.getGdDocsDaPubblicare(dbConn);
            Registro registroGddoc;

            for (GdDoc gddoc : listaGdDocsDaPubblicare) {

                if (mappaRegistri.containsKey(gddoc.getCodiceRegistro())) {
                    //se è già presente questo codice registro, allora utilizzo questo
                    registroGddoc = mappaRegistri.get(gddoc.getCodiceRegistro());
                } else {
                    //Altrimenti devo calcolarla e poi la inserisco nella mappa
                    Registro reg = Registro.getRegistro(gddoc.getCodiceRegistro(), dbConn);
                    registroGddoc = reg;
                    mappaRegistri.put(reg.getCodiceRegistro(), reg);
                }

                //Per ogni Gddoc recupero i sotto documenti da assegnare  alla pubblicazione
                log.debug("caricamento sottoDocumenti...");
                List<SottoDocumento> sottodocumenti = IodaDocumentUtilities.caricaSottoDocumenti(dbConn, gddoc.getId());
                log.debug("sottoDocumenti:");
                sottodocumenti.stream().forEach(s -> {
                    log.debug(s.toString());
                });

                //Per ogni GDDOC mi recupero le pubblicazioni associate. Sono solo quelle da pubblicare, opportunamente filtrate quando recuperiamo la lista dei Gddoc da pubblicare
                List<PubblicazioneIoda> pubblicazioniGdDoc = gddoc.getPubblicazioni();
                log.debug("pubblicazioni:");
                pubblicazioniGdDoc.stream().forEach(p -> {
                    log.debug(p.toString());
                });
                for (PubblicazioneIoda pubbIoda : pubblicazioniGdDoc) {
                    log.debug("pubblicazione: " + pubbIoda.toString());
                    switch (pubbIoda.getTipologia()) {
                        case ALBO:
                            pubblicaSuBalboAndUpdatePubbIoda(dbConn, gddoc, sottodocumenti, registroGddoc, pubbIoda);
                            break;
                        case POLITICO:
                        case DIRIGENTE:
                            if (possoPubblicare(dbConn, gddoc.getId(), pubbIoda))
                                pubblicaTrasparenzaSuBalboAndUpdatePubbIoda(dbConn, gddoc, registroGddoc, pubbIoda);
                            else
                                log.info("pubblicazione trasparenza non possibile perché il documento non è stato ancora pubblicato all'albo");
                            break;
                        case COMMITTENTE:
                            if (possoPubblicare(dbConn, gddoc.getId(), pubbIoda))
                                pubblicaCommittenteSuBalboAndUpdatePubbIoda(dbConn, gddoc, sottodocumenti, registroGddoc, pubbIoda);
                            break;
                        default:
                            log.error(String.format("tipologia pubblicazione %s non prevista", pubbIoda.getTipologia()));
                    }
//                    if (pubbIoda.getTipologia() == PubblicazioneIoda.Tipologia.ALBO) {
//                        pubblicaSuBalboAndUpdatePubbIoda(dbConn, gddoc, sottodocumenti, registroGddoc, pubbIoda);
//                    } else {
//                        if (possoPubblicare(dbConn, gddoc.getId(), pubbIoda)) {
//                            pubblicaTrasparenzaSuBalboAndUpdatePubbIoda(dbConn, gddoc, registroGddoc, pubbIoda);
//                        }
//                        else {
//                            log.info("pubblicazione trasparenza non possibile perché il documento non è stato ancora pubblicato all'albo");
//                        }
//                    }

                    // se tutto è ok faccio il commit
                    dbConn.commit();
                }
            } //Fine ciclo FOR GDDOC

        } //try
        catch (Throwable t) {
            log.fatal("Errore nel pubblicatore albo", t);
            //log.fatal("causa: ", t.getCause());
            //log.fatal(t.toString());
            //t.printStackTrace();
            try {
                if (dbConn != null) {
                    log.info("rollback...");
                    dbConn.rollback();
                }
            } catch (Exception ex) {
                log.error("errore nel rollback", ex);
            }
            throw new JobExecutionException(t);
        } finally {
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

    /**
     * se la pubblicazione da fare (di. solito "provvedimento") necessità della previa pubblicazione all'albo, controllo che questa esista
     * @param dbConn
     * @param idGdDoc
     * @param pubblicazioneCorrente
     * @return "true" se la pubblicazione corrente NON necessità di previa pubblicazione albo;
     *         "true" se la pubblicazione corrente necessità di previa pubblicazione albo e questa viene trovata.
     *         "false" se la pubblicazione corrente necessità di previa pubblicazione albo e questa NON viene trovata.
     * @throws SQLException 
     */
    private boolean possoPubblicare(Connection dbConn, String idGdDoc, PubblicazioneIoda pubblicazioneCorrente) throws SQLException {
        boolean possoPubblicare;
        log.info("controllo se posso pubblicare");
        if (pubblicazioneCorrente.isPubblicaSoloSePubblicatoAlbo()) {
            //possoPubblicare = pubblicazioniGdDoc.stream().anyMatch(p -> p.getNumeroPubblicazione() != null && p.getTipologia() == PubblicazioneIoda.Tipologia.ALBO);
            String sqlText = 
                    "SELECT id " +
                    "FROM gd.pubblicazioni_albo " +
                    "WHERE id_gddoc = ? AND " +
                    "tipologia = ? AND " +
                    "numero_pubblicazione is NOT NULL AND " +
                    "anno_pubblicazione is NOT NULL";
            try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
                ps.setString(1, idGdDoc);
                ps.setString(2, PubblicazioneIoda.Tipologia.ALBO.name());
                log.info("eseguo la query: " + ps.toString());
                ResultSet res = ps.executeQuery();
                possoPubblicare = res.next();
            }
        }
        else {
            possoPubblicare = true;
        }
        log.info("possoPubblicare: " + possoPubblicare);
        return possoPubblicare;
    }
    
    private void pubblicaSuBalboAndUpdatePubbIoda(Connection dbConn, GdDoc gddoc, List<SottoDocumento> sottodocumenti, Registro registroGddoc, PubblicazioneIoda pubbIoda) throws NamingException, ServletException, SQLException {
        log.debug("crezione pubblicazione per balbo...");
        PubblicazioneAlbo datiAlbo = new PubblicazioneAlbo(pubbIoda.getDataDal().toDate(), pubbIoda.getDataAl().toDate());

        DateTime dataEsecutivitaDateTime = pubbIoda.getDataEsecutivita();
        Date dataEsecutivita = null;
        if (pubbIoda.isEsecutiva()) {
            if (dataEsecutivitaDateTime == null) {
                dataEsecutivita = new Date();
            } else {
                dataEsecutivita = dataEsecutivitaDateTime.toDate();
            }
        }

        Pubblicazione pubblicazione = new Pubblicazione(
                gddoc.getAnnoRegistrazione(),
                gddoc.getNumeroRegistrazione(),
                registroGddoc.getCodiceRegistro(),
                registroGddoc.getDescPubbAlbo(),
                pubbIoda.getEsecutivita(),
                gddoc.getNomeStrutturaFirmatario(),
                UtilityFunctions.fixXhtml(gddoc.getOggetto()),
                "\"\"",
                "attivo",
                gddoc.getData().toDate(),
                gddoc.getDataRegistrazione().toDate(),
                dataEsecutivita,
                datiAlbo);

        // devo pubblicare solo i sotto documenti che hanno il flag "pubblicazione_albo" restituito da isPubblicazioneAlbo
        // filtro solo i sottoducumenti da pubblicare e per ognuno creo l'allegato pubblicazione e lo aggiungo alla pubblicazione
        sottodocumenti.stream().filter(
                s -> s.isPubblicazioneAlbo()).
                forEach(s -> {
                    log.info("crezione dell'AllegatoPubblicazione: " + s.getNome() + " - " + s.getCodiceSottoDocumento() + "...");
                    AllegatoPubblicazione allegatoPubblicazione = new AllegatoPubblicazione(s.getNome(), s.getCodiceSottoDocumento());
                    pubblicazione.addAllegato(allegatoPubblicazione);
                });

        /* 
                    // TEMPORANEO pubblico solo la stampa unica con omissis se c'è, sennò la stampa unica normale
                    // va cambiato usando un campo sul sottodocumento che indica se pubblicarlo oppure no
                    
                    // tira fuori le stampe uniche dai sotto documenti (se ci sono omissis saranno 2, altrimenti sarà solo una)
                    List<SottoDocumento> stampeUniche = sottodocumenti.stream().filter(s -> (s.getTipo().equals("stampa_unica") || s.getTipo().equals("stampa_unica_omissis"))).collect(Collectors.toList());
                    
                    // c'è la stampa unica omissis, prendo quella
                    if (stampeUniche.stream().anyMatch(s -> s.getTipo().equals("stampa_unica_omissis"))) {
                        SottoDocumento stampaUnicaOmissis = stampeUniche.stream().filter(s -> (s.getTipo().equals("stampa_unica_omissis"))).findAny().get();
                        if (stampaUnicaOmissis != null) {
                            log.debug("aggiunta allegato dal sottodocumento: " + stampaUnicaOmissis.toString());
                            AllegatoPubblicazione allegatoPubblicazione = new AllegatoPubblicazione(stampaUnicaOmissis.getNome(), stampaUnicaOmissis.getCodiceSottoDocumento());
                            pubblicazione.addAllegato(allegatoPubblicazione);
                        }
                        else {
                            log.error("dovrebbe esserci la stampa unica omissis, ma non c'è");
                            throw new ServletException("dovrebbe esserci la stampa unica omissis, ma non c'è");
                        }
                    }
                    // non c'è la stampa unica omissis, per cui prendo quella normale
                    else {
                        SottoDocumento stampaUnica = stampeUniche.stream().filter(s -> (s.getTipo().equals("stampa_unica"))).findAny().get();
                        if (stampaUnica != null) {
                            log.debug("aggiunta allegato dal sottodocumento: " + stampaUnica.toString());
                            AllegatoPubblicazione allegatoPubblicazione = new AllegatoPubblicazione(stampaUnica.getNome(), stampaUnica.getCodiceSottoDocumento());
                            pubblicazione.addAllegato(allegatoPubblicazione);
                        }
                        else {
                            log.error("dovrebbe esserci la stampa unica, ma non c'è");
                            throw new ServletException("dovrebbe esserci la stampa unica, ma non c'è");
                        }
                    }
         */
        log.debug("pubblicazione balbo: " + pubblicazione.toString());

        List<Pubblicazione> pubblicazioni = null;
        try {
            log.info("pubblicazione effettiva su balbo...");
            pubblicazioni = balboClient.pubblica(pubblicazione);
        } catch (org.springframework.web.client.HttpClientErrorException ex) {
            log.error("errore nella pubblicazione ", ex.getResponseBodyAsString());
            throw ex;
        }

        log.info("pubblicato, pubblicazioni balbo aggiornate: " + Arrays.toString(pubblicazioni.toArray()));

        // il pubblicatore mi torna una lista di pubblicazioni 
        // (una è quella appena pubblicata e le altre (una sola per ora?) sono quelle che sono state defisse)
        // per ogni pubblicazione tornata, controllo la data di defissione, se non c'è allora è quella appena pubblicata, altrimenti sono le defisse
        for (Pubblicazione p : pubblicazioni) {
            PubblicazioneIoda pIoda = new PubblicazioneIoda();
            pIoda.setNumeroPubblicazione(p.getNumeroPubblicazione());
            pIoda.setAnnoPubblicazione(p.getAnnoPubblicazione());
            if (p.getDataEsecutivita() != null)
                pIoda.setDataEsecutivita(new DateTime(p.getDataEsecutivita().getTime()));

            if (p.getDataDefissione() != null) { // pubblicazione defissa, aggiorno la data defissione (numero e anno ci sono già, quindi li uso per individuarla)
                pIoda.setDataDefissione(new DateTime(p.getDataDefissione().getTime()));
                log.info("update pubblicazione ioda locale per l'inserimento della data di defissione...");
                IodaDocumentUtilities.UpdatePubblicazioneByNumeroAndAnno(dbConn, p.getNumeroPubblicazione(), p.getAnnoPubblicazione(), pIoda);
                log.info("update pubblicazione ioda locale per l'inserimento della data di defissione completato");
            } else {
                pIoda.setPubblicatore(PUBBLICATORE);
                log.info("update pubblicazione ioda locale per l'inserimento del numero e l'anno...");
                IodaDocumentUtilities.UpdatePubblicazioneById(dbConn, pubbIoda.getId(), pIoda);
                log.info("update pubblicazione ioda locale per l'inserimento del numero e l'anno completato");
            }
        }
    }

    private void pubblicaTrasparenzaSuBalboAndUpdatePubbIoda(Connection dbConn, GdDoc gddoc, Registro registroGddoc, PubblicazioneIoda pubbIoda) throws NamingException, ServletException, SQLException {
        PubblicazioneTrasparenza pubblicazioneTrasparenza = new PubblicazioneTrasparenza();
        pubblicazioneTrasparenza.setArticolazione(gddoc.getNomeStrutturaFirmatario());        
        pubblicazioneTrasparenza.setCodiceRegistro(registroGddoc.getCodiceRegistro());
        pubblicazioneTrasparenza.setRegistro(registroGddoc.getDescPubbAlbo());
        pubblicazioneTrasparenza.setNumeroRegistro(gddoc.getNumeroRegistrazione());
        pubblicazioneTrasparenza.setAnnoRegistro(gddoc.getAnnoRegistrazione());
        pubblicazioneTrasparenza.setDataAdozione(gddoc.getDataRegistrazione().toDate());
        pubblicazioneTrasparenza.setOggetto(UtilityFunctions.fixXhtml(gddoc.getOggetto()));
        pubblicazioneTrasparenza.setTipoProvvedimento(pubbIoda.getTipoProvvedimentoTrasparenza());
        pubblicazioneTrasparenza.setTipologia(PubblicazioneTrasparenza.Tipologia.valueOf(pubbIoda.getTipologia().name()));

        try {
            log.info("pubblicazione effettiva su balbo trasparenza...");
            log.info("pubblicazioneTrasparenza: " + pubblicazioneTrasparenza.toString());
            pubblicazioneTrasparenza = balboClient.pubblicaTrasparenza(pubblicazioneTrasparenza);
        }
        catch (org.springframework.web.client.HttpClientErrorException ex) {
            log.error("errore nella pubblicazione trasparenza", ex.getResponseBodyAsString());
            throw ex;
        }

        log.info("update pubblicazione ioda locale per l'inserimento del numero e l'anno...");
        pubbIoda.setNumeroPubblicazione(pubblicazioneTrasparenza.getNumeroPubblicazione());
        pubbIoda.setAnnoPubblicazione(pubblicazioneTrasparenza.getAnnoPubblicazione());
        pubbIoda.setPubblicatore(PUBBLICATORE);

        IodaDocumentUtilities.UpdatePubblicazioneById(dbConn, pubbIoda.getId(), pubbIoda);
        log.info("update pubblicazione ioda locale per l'inserimento del numero e l'anno completato");
    }

    private void pubblicaCommittenteSuBalboAndUpdatePubbIoda(Connection dbConn, GdDoc gddoc, List<SottoDocumento> sottodocumenti, Registro registroGddoc, PubblicazioneIoda pubbIoda) throws NamingException, ServletException, SQLException, PubblicatoreException {
        DatiProfiloCommittente datiProfiloCommittente = IodaDocumentUtilities.getDatiProfiloCommittente(dbConn, gddoc.getIdOggettoOrigine());
        if (datiProfiloCommittente == null)
            throw new PubblicatoreException("dati del profilo committente non trovati");

        PubblicazioneCommittente pubblicazioneCommittente = new PubblicazioneCommittente();
        pubblicazioneCommittente.setArticolazione(gddoc.getNomeStrutturaFirmatario());        
        pubblicazioneCommittente.setCodiceRegistro(registroGddoc.getCodiceRegistro());
        pubblicazioneCommittente.setRegistro(registroGddoc.getDescPubbAlbo());
        pubblicazioneCommittente.setNumeroRegistro(gddoc.getNumeroRegistrazione());
        pubblicazioneCommittente.setAnnoRegistro(gddoc.getAnnoRegistrazione());
        pubblicazioneCommittente.setDataRegistrazione(gddoc.getDataRegistrazione().toDate());
        pubblicazioneCommittente.setOggetto(UtilityFunctions.fixXhtml(gddoc.getOggetto()));
        pubblicazioneCommittente.setAggiudicatario(datiProfiloCommittente.getAggiudicatario());
        pubblicazioneCommittente.setCheckFornitoreRequisitiGenerali(datiProfiloCommittente.getCheckFornitoreRequisitiGenerali());
        pubblicazioneCommittente.setCheckFornitoreRequisitiProfessionali(datiProfiloCommittente.getCheckFornitoreRequisitiProfessionali());
        pubblicazioneCommittente.setCig(datiProfiloCommittente.getCig());
        pubblicazioneCommittente.setCigAzienda(datiProfiloCommittente.getCigAzienda());
        pubblicazioneCommittente.setCodiceProfiloCommittente(PubblicazioneCommittente.CodiceProfiloCommittente.fromString(datiProfiloCommittente.getCodiceProfiloCommittente()));
        pubblicazioneCommittente.setTestoProfiloCommittente(datiProfiloCommittente.getTestoProfiloCommittente());
        
        DateTime dataAggiudicazione = datiProfiloCommittente.getDataAggiudicazione();
        if (dataAggiudicazione != null) {
            pubblicazioneCommittente.setDataAggiudicazione(dataAggiudicazione.toDate());
        }
        pubblicazioneCommittente.setFornitore(datiProfiloCommittente.getFornitore());
        pubblicazioneCommittente.setImporto(datiProfiloCommittente.getImporto());
        pubblicazioneCommittente.setOggettoAffidamento(datiProfiloCommittente.getOggettoAffidamento());
        pubblicazioneCommittente.setOperatoriEconomiciInviati(datiProfiloCommittente.getOperatoriEconomiciInviati());
        pubblicazioneCommittente.setOperatoriEconomiciOfferenti(datiProfiloCommittente.getOperatoriEconomiciOfferenti());
        pubblicazioneCommittente.setProcedura(datiProfiloCommittente.getProcedura());
        pubblicazioneCommittente.setRagioniSceltaFornitore(datiProfiloCommittente.getRagioniSceltaFornitore());
        pubblicazioneCommittente.setTipologiaGara(datiProfiloCommittente.getTipologiaGara());
        
        // devo pubblicare solo i sotto documenti che hanno il flag "pubblicazione_albo" restituito da isPubblicazioneAlbo (mi aspetto che sia solo la stampa unica), se ne trovo zero o più di 1 dò errore
        // filtro solo i sottoducumenti da pubblicare
        List<SottoDocumento> sottoDocumentiDaPubblicare = 
                sottodocumenti.stream().filter(s -> s.isPubblicazioneAlbo()).collect(Collectors.toList());
        
        if (sottoDocumentiDaPubblicare == null || sottoDocumentiDaPubblicare.isEmpty()) {
            String message = "nessun sottodocumento da pubblicare";
            log.error(message);
            throw new PubblicatoreException(message);
        }
        else if (sottoDocumentiDaPubblicare.size() > 1) {
            String message = "troppi sottodocumenti da pubblicare, me ne aspetto solo uno";
            log.error(message);
            throw new PubblicatoreException(message);
        }
        
        // setto i dati del sottodocumento sulla pubblicazione
        for (SottoDocumento s : sottoDocumentiDaPubblicare) {
            AllegatoPubblicazione allegatoPubblicazione = new AllegatoPubblicazione(s.getNome(), s.getCodiceSottoDocumento());
            log.info(String.format("inserimento dei dati per la visualizzazione della stampa unica: nomeDocumento: %s codiceDocumento: %s ...",s.getNome(), s.getCodiceSottoDocumento()));
            pubblicazioneCommittente.addAllegato(allegatoPubblicazione);
        }

        try {
//            synchronized (sinc) {
            log.info("pubblicazione effettiva su balbo committente...");
            log.info("pubblicazioneCommittente: " + pubblicazioneCommittente.toString());
            pubblicazioneCommittente = balboClient.pubblicaCommittente(pubblicazioneCommittente);
//            }
        }
        catch (org.springframework.web.client.HttpClientErrorException ex) {
            log.error("errore nella pubblicazione committente", ex.getResponseBodyAsString());
            throw ex;
        }

        log.info("update pubblicazione ioda locale per l'inserimento del numero e l'anno...");
        pubbIoda.setNumeroPubblicazione(pubblicazioneCommittente.getNumeroPubblicazione());
        pubbIoda.setAnnoPubblicazione(pubblicazioneCommittente.getAnnoPubblicazione());
        pubbIoda.setPubblicatore(PUBBLICATORE);

        IodaDocumentUtilities.UpdatePubblicazioneById(dbConn, pubbIoda.getId(), pubbIoda);
        log.info("update pubblicazione ioda locale per l'inserimento del numero e l'anno completato");
    }

}
