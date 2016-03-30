package it.bologna.ausl.bds_tools.jobs;

//import it.bologna.ausl.balboclient.BalboClient;
//import it.bologna.ausl.balboclient.classes.AllegatoPubblicazione;
//import it.bologna.ausl.balboclient.classes.Pubblicazione;
//import it.bologna.ausl.balboclient.classes.PubblicazioneAlbo;
//import it.bologna.ausl.balboclient.classes.PubblicazioneTrasparenza;
import it.bologna.ausl.balboclient.BalboClient;
import it.bologna.ausl.balboclient.classes.AllegatoPubblicazione;
import it.bologna.ausl.balboclient.classes.Pubblicazione;
import it.bologna.ausl.balboclient.classes.PubblicazioneAlbo;
import it.bologna.ausl.balboclient.classes.PubblicazioneTrasparenza;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.Registro;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.PubblicazioneIoda;
import it.bologna.ausl.ioda.iodaobjectlibrary.SottoDocumento;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
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

            String query = ApplicationParams.getGdDocToPublishExtractionQuery();
            log.debug("query caricata");

            log.debug("preparedStatement per select...");
            ps = dbConn.prepareStatement(query);
            log.debug("preparedStatement per select settata");

            log.debug("sto per eseguire select. Query: " + ps.toString());
            ResultSet res = ps.executeQuery();
            log.debug("query eseguita");

            LocalDateTime dataOdierna = LocalDateTime.now();
            ZonedDateTime timeZoneDal = dataOdierna.atZone(ZoneId.systemDefault());
            Date dataDal = Date.from(timeZoneDal.toInstant());

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
                
                //Per ogni GDDOC mi recupero le pubblicazioni associate. Sono solo quelle da pubblicare, opportunamente filtrate quando recuperiamo la lista dei Gddoc da pubblicare
                List<PubblicazioneIoda> pubblicazioniGdDoc = gddoc.getPubblicazioni();
                Pubblicazione pubblicazione = null;
                ArrayList<Pubblicazione> pubblicazioniList = new ArrayList<>();
                ArrayList<PubblicazioneIoda> pubblicazioniDaAggiornare = new ArrayList<>();
                
                //Per ogni Gddoc recupero i sotto documenti da assegnare  alla pubblicazione
                List<SottoDocumento> sottodocumenti = IodaDocumentUtilities.caricaSottoDocumenti(dbConn, gddoc.getId());
                
                for(PubblicazioneIoda pubbIoda : pubblicazioniGdDoc){
                
                    PubblicazioneAlbo datiAlbo = new PubblicazioneAlbo(pubbIoda.getDataDal().toDate(),pubbIoda.getDataAl().toDate());
                    
                                       
                    pubblicazione = new Pubblicazione(
                            gddoc.getAnnoRegistrazione(),  
                            gddoc.getNumeroRegistrazione(), 
                            registroGddoc.codiceRegistro, 
                            registroGddoc.descPubbAlbo, 
                            "esecutiva", 
                            gddoc.getNomeStrutturaFirmatario(), 
                            UtilityFunctions.fixXhtml(gddoc.getOggetto()), 
                            gddoc.getTipoOggettoOrigine(), 
                            "attivo", 
                            gddoc.getData().toDate(), 
                            gddoc.getDataRegistrazione().toDate(),
                            gddoc.getDataRegistrazione().toDate(), 
                            datiAlbo, 
                            null);
    
                    //Ad ogni pubblicazione, assegno i sottodocumenti del gddoc a cui appartengono
                    for (SottoDocumento sottodoc : sottodocumenti) {
                    
                        AllegatoPubblicazione allegatoPubblicazione = new AllegatoPubblicazione(sottodoc.getNome(), sottodoc.getCodiceSottoDocumento());
                        pubblicazione.addAllegato(allegatoPubblicazione);
                    }
                    
                    pubblicazioniList.add(pubblicazione);
                }
                
                //Ciclo sulla lista di pubblicazioni e le pubbllico per davvero
                for (Pubblicazione pubb : pubblicazioniList) {
                    
                    Pubblicazione p = balboClient.pubblica(pubb);
                    PubblicazioneAlbo pAlbo = p.getDatiAlbo();
                    pubblicazioniDaAggiornare.add(new PubblicazioneIoda(p.getNumeroPubblicazione(), 
                            p.getAnnoPubblicazione(), 
                            new DateTime(pAlbo.getPubblicazioneDal()), 
                            new DateTime(pAlbo.getPubblicazioneAl()), 
                            Document.DocumentOperationType.UPDATE));
                }
                
                //Al termine di tutte le pubblicazioni, le aggiorno anche nel gddoc
                gddoc.setPubblicazioni(pubblicazioniDaAggiornare);
                       
            } //Fine ciclo FOR GDDOC
            
            
            
            //TODO ELIMINARE IL WHILE PERCHE' ORA UTILIZZO LA FUNZIONE CHE RESTITUISCE I GDDOC DA CON LE PUBBLICAZIONI DA PUBBLICARE
//            while (res.next()) {
//                String idGdDoc = res.getString("id_gddoc");
//                String idUtenteModifica = res.getString("id_utente_modifica");
//                String nomeGdDoc = res.getString("nome_gddoc");
//                String categoriaOrigine = res.getString("categoria_origine");
//                String tipoGddoc = res.getString("tipo_gddoc");
//                String dataUltimaModifica = res.getString("data_ultima_modifica");
//                String statoGdDoc = res.getString("stato_gd_doc");
//                String inUsoDa = res.getString("in_uso_da");
//                Date dataGddoc = res.getTimestamp("data_gddoc");
//                String guidGddoc = res.getString("guid_gddoc");
//                String codiceRegistro = res.getString("codice_registro_gd");
//                Date dataRegistrazione = res.getTimestamp("data_registrazione");
//                String numeroRegistrazione = res.getString("numero_registrazione");
//                String idUtenteRegistrazione = res.getString("id_utente_registrazione");
//                Integer annoRegistrazione = res.getInt("anno_registrazione");
//                String firmato = res.getString("firmato");
//                String idOggettoOrigine = res.getString("id_oggetto_origine");
//                String tipoOggettoOrigine = res.getString("tipo_oggetto_origine");
//                String oggetto = res.getString("oggetto_gddoc");
//                String codice = res.getString("codice");
//                String descrizioneRegistro = res.getString("descrizione_albo");
////                String contenutoTrasparenza = res.getString("contenuto_trasparenza");
////                String oggettoTrasparenza = res.getString("oggetto_trasparenza");
////                String spesaPrevista = res.getString("spesa_prevista");
////                String estremiDocumento = res.getString("estremi_documento");
////                String tipoProvvedimento = res.getString("tipo_provvedimento_trasparenza");
//                String articolazione = res.getString("articolazione");
//                Integer giorniPubblicazione = res.getInt("giorni_pubblicazione");
//                String nomeSottoDocumento = res.getString("nome_sottodocumento");
//                String codiceSottoDocumento = res.getString("codice_sottodocumento");
//                String idApplicazione = res.getString("id_applicazione");
//
//                ps.close();
//                log.debug("prepared statement per select chiuso");
//                
//                LocalDateTime dataAlLocalDate = dataOdierna.plusDays(giorniPubblicazione);
//                ZonedDateTime timeZoneAl = dataAlLocalDate.atZone(ZoneId.systemDefault());
//                Date dataAl = Date.from(timeZoneAl.toInstant());
//
//                log.debug("setto PubblicazioneAlbo...");
//                PubblicazioneAlbo datiAlbo = new PubblicazioneAlbo(dataDal, dataAl);
//                log.debug("settato PubblicazioneAlbo");
//                PubblicazioneTrasparenza datiTrasparenza = null;
//
//                log.debug("controllo tipo provvedimento");
////                if ((tipoProvvedimento != null) && (!tipoProvvedimento.equalsIgnoreCase(TIPO_PROVVEDIMENTO_NON_RILEVANTE))) {
////                    datiTrasparenza = new PubblicazioneTrasparenza(oggettoTrasparenza, spesaPrevista, estremiDocumento);
////                } else {
////                    log.debug("setto tipoProvvedimento a vuoto");
////                    tipoProvvedimento = "";
////                }
//
//                log.debug("setto tipoProvvedimento a vuoto");
//                String tipoProvvedimento = "";
//
//                log.debug("setto Pubblicazione...");
//                Pubblicazione pubblicazione = new Pubblicazione(annoRegistrazione,
//                        numeroRegistrazione,
//                        descrizioneRegistro,
//                        "esecutiva",
//                        articolazione,
//                        oggetto,
//                        tipoProvvedimento,
//                        "attivo",
//                        dataGddoc,
//                        dataRegistrazione,
//                        dataRegistrazione,
//                        datiAlbo,
//                        datiTrasparenza);
//                log.debug("settata Pubblicazione");
//
//                // allego la stampa unica come allegato
//                log.debug("setto AllegatoPubblicazione...");
//                AllegatoPubblicazione allegatoPubblicazione = new AllegatoPubblicazione(nomeSottoDocumento, codiceSottoDocumento);
//                log.debug("settato AllegatoPubblicazione");
//                log.debug("aggiungo allegato...");
//                pubblicazione.addAllegato(allegatoPubblicazione);
//                log.debug("allegato inserito");
//
//                log.debug("sto per pubblicare...");
//                pubblicazione = balboClient.pubblica(pubblicazione);
//                log.debug("pubblicato: " + pubblicazione.toString());
//
//                //QUesta parte deve essere sostituita con il ciclo sulla collection gddoc e aggiornamento delle pubblicazioni.
//                log.debug("preparo query di updateGdDoc...");
//                String queryUpdatePubblicato = ApplicationParams.getGdDocUpdatePubblicato();
//                // queryUpdatePubblicato = queryUpdatePubblicato.replace("?", "'" + idGdDoc + "'");
//                log.debug("preparedStatement per updateGdDoc...");
//                ps = dbConn.prepareStatement(queryUpdatePubblicato);
//                log.debug("preparedStatement per updateGdDoc settato");
//
//                ps.setString(1, idGdDoc);
//                log.debug("sto per eseguire update. Query: " + ps.toString());
//                ps.executeUpdate();
//                log.debug("eseguito update");
//
//                ps.close();
//                log.debug("prepared statement per updateGdDoc chiuso");
//
//
////                log.debug("preparo query di insert PubblicazioneAlbo...");
////                String queryInsertPubblicazioneAlbo = ApplicationParams.getPubblicazioniAlboQueryInsertPubblicazione();
////                log.debug("preparedStatement per insert PubblicazioneAlbo...");
////                ps = dbConn.prepareStatement(queryInsertPubblicazioneAlbo);
////                log.debug("preparedStatement per insert PubblicazioneAlbo settato");
////
////                log.debug("recupero relata...");
////                AllegatoPubblicazione relata = pubblicazione.getAllegatiPubblicazione().stream().filter(a -> a.isRelata()).findFirst().get();
////                log.debug("relata recuperata: " + relata.toString());
////                
////                ps.setString(1, String.valueOf(pubblicazione.getId()));
////                ps.setString(2, idApplicazione);
////                ps.setTimestamp(3, new Timestamp(pubblicazione.getDatiAlbo().getPubblicazioneDal().getTime()));
////                ps.setTimestamp(4, new Timestamp(pubblicazione.getDatiAlbo().getPubblicazioneAl().getTime()));
////                ps.setString(5, relata.getUuid());
////                ps.setString(6, guidGddoc);
////                ps.setInt(7, -1);
////                ps.setTimestamp(8, new Timestamp(pubblicazione.getDataPubblicazione().getTime()));
////                log.debug("sto per eseguire insert PubblicazioneAlbo. Query: " + ps.toString());
////                ps.executeUpdate();
////                log.debug("eseguito insert PubblicazioneAlbo");
////                ps.close();
////                log.debug("prepared statement per insert PubblicazioneAlbo chiuso");
//             
//                dbConn.commit();
//            }

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

//    public void setIntervalDays(int hour) {
//        this.intervalDays = hour;
//    }
//
//    public void setIntervalDays(String hour) {
//        this.intervalDays = Integer.valueOf(hour);
//    }
}
