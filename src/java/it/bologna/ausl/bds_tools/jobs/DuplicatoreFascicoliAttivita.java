package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.SetDocumentNumber;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
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
public class DuplicatoreFascicoliAttivita implements Job{
    private static final Logger log = LogManager.getLogger(DuplicatoreFascicoliAttivita.class);
    
    // nome della sequenza per la numerazione, è passata come parametro nel json dello schedulatore. Viene settata in auomatico in fase di inizializzazione
    private String sequenceName;
    
    private final int SQL_TRUE = -1;
    private final int SQL_FALSE = 0;
    private final String ID_APPLICAZIONE = "gedi";
    private final int TIPO_FASCICOLO_ATTIVITA = 3;
    private final String STATO_FASCICOLO_APERTO = "a";

    private final String LIVELLO_FASCICOLO_RADICE = "1";
    private final String LIVELLO_FASCICOLO_SOTTOFASCICOLO = "2";
    private final String LIVELLO_FASCICOLO_INSERTO = "3";
    
    private final int MAX_LIVELLO_FASCICOLO = 3;

    // colonne da escludere nella copia automatica del fascicolo
    private static final String[] COLUMN_TO_EXCLUDE = {"id_fascicolo","nome_fascicolo","numero_fascicolo","anno_fascicolo","id_fascicolo_padre",
        "numerazione_gerarchica","guid_fascicolo","stato_mestieri","in_uso_da","id_padre_catena_fascicolare","codice_fascicolo","tscol", "data_registrazione", 
        "id_fascicolo_importato", "data_chiusura", "note_importazione", "fascicolo_pregresso_collegato", "note",
        "copiato_da", "servizio_creazione"};
    
    private int currYear;

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }
    
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
//        if (true)
//            return;
        try (Connection dbConn = UtilityFunctions.getDBConnection();) {

            LocalDateTime now = LocalDateTime.now();
            currYear = now.getYear();
            try {
                // lancio il servizio solo se non è stato già fatto quest'anno, basandomi sulla data esecuzione salvata nella tabella bds_tools.servizi.
                if (notDoneThisYear(dbConn)) {
                    // Inoltre effettuo un ulteriore controllo di sicurezza per evitare di eseguirlo se è già stato eseguito, controllando che non eistano fascicoli creati quest'anno con questo servizio.
                    if (securityCheck(dbConn))  {
                        // infine lancio il servizio solo se esiste il fascicolo speciale per l'anno corrente, altrimenti, con la numerazione prenderei il numero 1, che è riservato al fascilo speciale.
                        if (existFascicoloSpeciale(dbConn)) {
                            log.info(String.format("fascicolo speciale 1 dell'anno corrente %s trovato, posso proseguire", currYear));
                            log.debug("=========================== Avvio Duplicazione Fascicoli Attività ===========================");
                            
                            // scrivo la data di inizio del servizio sulla tabella bds_tools.servizi, lo faccio prima dell'inizio della transazione in modo che se poi c'è un errore, comunque rimane l'ora dell'ultimo tentativo
                            updateDataInizioDataFine(dbConn, Timestamp.valueOf(now), null);
                            
                            // avvio la transazione
                            dbConn.setAutoCommit(false);
                            log.info("avvio duplicazione");
                            
                            // avvio duplicazione
                            duplicateFascicoli(dbConn);
                            log.info("duplicazione terminata");
                            
                            // aggiorno la data di terminazione del servizio
                            updateDataInizioDataFine(dbConn, null, Timestamp.valueOf(LocalDateTime.now()));
                            
                            // commit della transazione
                            dbConn.commit();
                            log.debug("=========================== Fine Duplicazione Fascicoli Attività ===========================");
                        }
                        else {
                            log.info("fascicolo speciale 1 dell'anno corrente NON trovato, impossibile procedere alla duplicazione");
                        }
                    }
                    else {
                        log.error(String.format("si sta cercando di ricreare i fascicoli di tipo attività dell'anno :%s, ma esistono già. Non faccio niente", currYear));
                    }
                }
                else {
                    log.info(String.format("duplicazione fascicoli già eseguita per l'anno corrente: %s", currYear));
                }
            }
            catch (Exception ex) {
                log.error("Errore nella duplicazione dei fascicoli: ", ex);
                // se c'è un qualsiasi errore faccio il rollback
                if (!dbConn.getAutoCommit())
                    dbConn.rollback();
                throw ex;
            }
        }
        catch (Exception ex) {
            log.error("Errore nel servizio di duplicazione dei fascicoli: ", ex);
        }
    }

    /**
     * Controlla se esiste il fascicolo speciale di quest'anno
     * @param dbConn
     * @return
     * @throws SQLException 
     */
    private boolean existFascicoloSpeciale(Connection dbConn) throws SQLException {
        String query = ""
                + "select id_fascicolo "
                + "from gd.fascicoligd "
                + "where numero_fascicolo = ? and "
                + "anno_fascicolo = ? and "
                + "speciale = ? and "
                + "id_livello_fascicolo = ?";
        
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            ps.setInt(1, 1);
            ps.setInt(2, currYear);
            ps.setInt(3, SQL_TRUE);
            ps.setString(4, LIVELLO_FASCICOLO_RADICE);
            
            log.debug(String.format("verifico l'esistenza del fascicolo speciale con numero 1 dell'anno corrente(%s) ", currYear));
            log.debug(String.format("eseguo la query: %s...", ps.toString()));
            ResultSet res = ps.executeQuery();
            return res.next();
        }
    }

    /**
     * Controllo di sicurezza: controlla che non esista nessun fascicolo creato da questo servizio per quest anno.
     * Torna "true" se i fascicoli effettimavamente non sono stati ancora duplicati, "false" altrimenti
     * @param dbConn
     * @return "true" se i fascicoli effettimavamente non sono stati ancora duplicati, "false" altrimenti
     */
    private boolean securityCheck(Connection dbConn) throws SQLException {
        String query = ""
                + "select id_fascicolo "
                + "from gd.fascicoligd "
                + "where servizio_creazione = ? and anno_fascicolo = ? and id_livello_fascicolo =?";
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            ps.setString(1, getClass().getSimpleName());
            ps.setInt(2, currYear);
            ps.setString(3, LIVELLO_FASCICOLO_RADICE);
            log.debug(String.format("eseguo la query: %s...", ps.toString()));
            ResultSet res = ps.executeQuery();
            return !res.next();
        }
    }

    /**
     * Controlla che il servizio non sia stato già eseguito quest'anno. Per farlo legge l'anno della data di fine dalla tabella bds_tools.servizi
     * @param dbConn
     * @return "true" se il servizio non è stato eseguito quest'anno, "false" altrimenti
     * @throws SQLException 
     */
    private boolean notDoneThisYear(Connection dbConn) throws SQLException {
        String query = ""
                + "select extract (year from data_fine) "
                + "from bds_tools.servizi "
                + "where nome_servizio = ? and id_applicazione = ?";
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            ps.setString(1, getClass().getSimpleName());
            ps.setString(2, ID_APPLICAZIONE);
            log.debug(String.format("eseguo la query: %s", ps.toString()));
            ResultSet res = ps.executeQuery();
            if (res.next()) {
                int year = res.getInt(1);
                log.debug(String.format("anno letto: %s", year));
                
                // se l'anno letto è minore dell'anno corrente allora il servizio non è stato ancora eseguito.
                // NB: se la data è NULL year è 0
                return year < currYear;
            }
            else
                throw new SQLException(String.format("servizio %s dell'applicazione %s non trovato", getClass().getSimpleName(), ID_APPLICAZIONE));
        }
    }
    
    /**
     * aggiorna le date di avvio, termine del servizio, per aggiorarne solo una passare null all'altra
     * @param dataInizio
     * @param dataFine
     * @throws SQLException 
     */
    private void updateDataInizioDataFine(Connection dbConn, Timestamp dataInizio, Timestamp dataFine) throws SQLException {
        String query = ""
                + "update bds_tools.servizi "
                + "set "
                + "data_inizio = coalesce(?, data_inizio), "
                + "data_fine = coalesce(?, data_fine) "
                + "where nome_servizio = ? and id_applicazione = ?";
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            int index = 1;
            ps.setTimestamp(index++, dataInizio);
            ps.setTimestamp(index++, dataFine);
            ps.setString(index++, getClass().getSimpleName());
            ps.setString(index++, ID_APPLICAZIONE);
            
            log.debug(String.format("eseguo la query: %s...", ps.toString()));
            int res = ps.executeUpdate();
            if (res == 0)
                throw new SQLException("errore nell'aggiornamento delle date inizio/fine, l'update ha tornato 0");
        }
    }

    /**
     * Estrare i fascicoli di tipo attività del livello passato, relativi all'anno scorso e con stato "aperto" ("a")
     * Nel caso di livello > 1 i fascicoli saranno tutti i fascicoli di qualsiasi tipo (anche non attività) e di qualsiasi anno discendenti di fascicoli radice di tipo attività
     * @param dbConn
     * @param ps passare un PreparedStatement, anche null; lo valorizza e lo usa la funzione
     * @param livello
     * @return il ResultSet dei fascicoli estratti
     * @throws SQLException 
     */
    private ResultSet extractFascicoliAttivita(Connection dbConn, PreparedStatement ps, String livello) throws SQLException {
        String query = ""
                + "select * from gd.fascicoligd "
                + "where id_fascicolo in"
                + "("
                +   "with recursive fascicoli_ric as"
                +   "("
                +       "select id_fascicolo, id_fascicolo as radice, id_livello_fascicolo "
                +       "from gd.fascicoligd "
                +       "where "
                +       "id_fascicolo_padre is null and id_tipo_fascicolo = ? and anno_fascicolo = ? and stato_fascicolo = ?"
                +       "union "
                +       "select s.id_fascicolo, r.radice, s.id_livello_fascicolo "
                +       "from gd.fascicoligd s join fascicoli_ric r on s.id_fascicolo_padre = r.id_fascicolo "
                +   ")"
                +   "select id_fascicolo "
                +   "from fascicoli_ric "
                +   "where id_livello_fascicolo = ?"
                + ")";
        ps = dbConn.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ps.setInt(1, TIPO_FASCICOLO_ATTIVITA);
        ps.setInt(2, currYear - 1);
        ps.setString(3, STATO_FASCICOLO_APERTO);
        ps.setString(4, livello);
        log.debug(String.format("eseguo la query %s...", ps.toString()));

        return ps.executeQuery();
    }

    /**
     * Esegue la duplicazione vera è propria.
     * Passi della duplicazione:
     * Cicla su ogni livello, a partire da 1 fino a 3 (fascicoli, sotto fascicoli e inserti)
     *  Per ogni livello estraggo i fascicoli del livello e per ognuno inserisco un nuovo fascicolo in cui setto prima i campi che non posso duplicare (es.
     *  id_fascicolo, guid_fascicolo, ecc.) e poi duplico tutti gli altri campi (duplico tutti i campi ad eccezione di quelli indicati nell'array COLUMN_TO_EXCLUDE)
     *  Durante la duplicazione salvo in una mappa l'id del fascicolo che sto duplicando e l'id di quello duplicato, che userò al giro successivo per settare
     *  l'id_fascicolo_padre sul fascicolo figlio che duplicherò.
     *  A fine duplicazione, duplico anche i vicari.
     *  Infine se il fascicolo è un fascicolo radice allora lo numero, usando il servizio di numerazione.
     * @param dbConn
     * @throws SQLException
     * @throws IOException
     * @throws MalformedURLException
     * @throws SendHttpMessageException
     * @throws ServletException 
     */
    private void duplicateFascicoli(Connection dbConn) throws SQLException, IOException, MalformedURLException, SendHttpMessageException, ServletException {

        /* 
            nella duplicazione ho bisono di inserire il campo id_fascicolo_padre; per recuperarlo dovrei fare una query che mi recuperi il campo id_fasicolo 
            dal fascicolo che ha id_fascicolo_duplicato = al id_fascicolo_padre del fascicolo che sto duplicando.
            Per evitare di fare una query per ogni fascicolo che voglio duplicare allora, ad ogni duplicazione, salvo in una mappa 
            id fascicolo che sto duplicando -> id_fascicolo duplicato. Questo mi permette, al giro successivo di prendere il campo dell'id_fascicolo_padre 
            direttamente dalla mappa. Infatti l'id_fascicolo_padre del fascicolo duplicato che sto creando, sarà il valore identificato dalla chiave 
            id_fascicolo_padre del fascicolo che sto duplicando.
            Ho bisogno quindi di 2 mappe: una in cui scrivere la mappatura degli id_fascicolo (sorgente->duplicato) e una da cui leggerli. 
            Ad ogni ciclo devo quindi scrivere in una e leggere dall'altra (che deve essere quella che nel giro precedente è stata scritta)
            Per realizzare questo utilizzo un array di 2 elementi in cui inserisco le reference delle 2 mappe e ad ogni ciclo le utilizzo per leggere/scrivere
            in modo alternato. Naturalmente al primo giro, non leggerò niente, e all'ultimo non scriverò niente.
        */
        Map<String, String> idFascicoliOldNewMap1 = new HashMap();
        Map<String, String> idFascicoliOldNewMap2 = new HashMap();
        Map<String, String>[] idFascicoliMaps = new Map[2];
        idFascicoliMaps[0] = idFascicoliOldNewMap1;
        idFascicoliMaps[1] = idFascicoliOldNewMap2;

        // ciclo su tutti i livelli
        for(int livello = 1; livello <= MAX_LIVELLO_FASCICOLO; livello++) {
            log.info(String.format("livello: %d", livello));
            try (PreparedStatement ps = null) {
                // estraggo i fascicoli del livello corrente
                ResultSet fascicoliRS = extractFascicoliAttivita(dbConn, ps, String.valueOf(livello));
                int fascicoliNumber;
                if (fascicoliRS.last()) {
                    fascicoliNumber = fascicoliRS.getRow();
                    log.info(String.format("fascioli di livello %d trovati: %d", livello, fascicoliNumber));
                    fascicoliRS.beforeFirst();
                    JSONArray indeIds = UtilityFunctions.getIndeId(fascicoliNumber);

                    // per alternare l'uso delle mappe, leggo dal vettore in posizione (livello % 2)
                    // es. al livello 1 
                    // (1 % 2) = 1 (leggerò dalla mappa alla posizione 1) [in realtà al giro 1 non leggerò perché sono al livello radice e non ho fascicoli padre]
                    // 1 - (1 % 2) = 0 (scriverò nella mappa alla posizione 0)
                    //-------------------
                    // es. al livello 2
                    // (2 % 2) = 0 (leggerò dalla mappa alla posizione 0)
                    // 1 - (2 % 2) = 1 (scriverò nella mappa alla posizione 1)
                    //-------------------
                    // es. al livello 3
                    // (3 % 2) = 1 (leggerò dalla mappa alla posizione 1)
                    // 1 - (3 % 2) = 0 (scriverò nella mappa alla posizione 0) [in realtà al giro 3 che è l'ultimo non scriverò perché gli inserti non possono essere padri di nessuno]
                    
                    int readMapIndex = (livello % 2);
                    int writeMapIndex = 1 - (livello % 2);
                    Map<String, String> readIdMap = idFascicoliMaps[readMapIndex];
                    Map<String, String> writeIdMap = idFascicoliMaps[writeMapIndex];
//                    log.debug(String.format("readMapIndex: %d", readMapIndex));
//                    log.debug(String.format("writeMapIndex: %d", writeMapIndex));

                    // svuoto la mappa in cui scrivo, per ripulirla dai dati del giro precedente
                    writeIdMap.clear();
                    int fascicoliCont = 0;
                    log.info(String.format("inizio duplicazione livello %d", livello));
                    /*
                        Ciclo su tutti i fascicoli estratti.
                        Devo aggiungere la condizione "fascicoliCont < fascicoliNumber" perché uso la modalità di inserimento del RecodSet:
                        Questa modalità mi permette di inserire facilmente senza dover genrare la stringa "INSERT INTO..." ed eseguire la query per l'inserimento.
                        Così facendo però inserisco il nuovo oggetto sia nel database, che nel RecordSet che sto ciclando, perciò, alla fine degli oggetti
                        tirati fuori dalla query, mi ritroverò i nuovi oggetti inseriti; per terminare il ciclo alla fine degli oggetti iniziali e non proseguire
                        con quelli appena inseriti devo quindi contare il numero di oggetti presenti inizialmente e aggiungere una condizione che mi faccia terminare
                        il ciclo quando ne raggiungo il numero.
                    */
                    while(fascicoliRS.next() && fascicoliCont < fascicoliNumber) {
                        String idFascicoloSorgente = fascicoliRS.getString("id_fascicolo");
                        String numerazioneGerarchicaSorgente = fascicoliRS.getString("numerazione_gerarchica");
                        log.info(String.format("processo il fascicolo %s con id: %s", numerazioneGerarchicaSorgente, idFascicoloSorgente));
                        
                        // sposto il cursore in modalità inserimento (dopo l'inserimento lo rimetterò nella posizione corrente)
                        fascicoliRS.moveToInsertRow();

                        // chiedo alla servlet INDE degli id/guid per inserire i fascicoli che devo duplicare. Chiedo tanti id quanti fascicoli ho estratto(naturalmente)
                        JSONObject idAndGuid = (JSONObject)indeIds.get(fascicoliCont);
                        
                        String idFascicoloDuplicato = (String) idAndGuid.get("document_id");
                        String guidFascicoloDuplicato = (String) idAndGuid.get("document_guid");
                        fascicoliRS.updateString("id_fascicolo", idFascicoloDuplicato);

                        fascicoliRS.updateInt("anno_fascicolo", currYear);
                        fascicoliRS.updateString("guid_fascicolo", guidFascicoloDuplicato);
                        
                        // il codice fascicolo contiene il guid del fascicolo, per cui calcolo il codice prendendo il codice del fascicolo sorgente e sostituendone il guid con quello di quello duplicato
                        fascicoliRS.updateString("codice_fascicolo", fascicoliRS.getString("codice_fascicolo").replace(fascicoliRS.getString("guid_fascicolo"), guidFascicoloDuplicato));
                        fascicoliRS.updateString("copiato_da", idFascicoloSorgente);
                        // nel servizio_creazione ci scrivo il nome della classe
                        fascicoliRS.updateString("servizio_creazione", getClass().getSimpleName());

                        // se non sono al livello 1 reperisco il fascicolo padre dalla mappa (come spiegato precedentemente)
                        String idFascicoloPadre = null;
                        if (livello > 1) {
                            log.info("reperimento id_fascicolo_padre");
                            idFascicoloPadre = readIdMap.get(fascicoliRS.getString("id_fascicolo_padre"));
                            
//                            log.debug(String.format("id_fascicolo_padre sorgente = %s", fascicoliRS.getString("id_fascicolo_padre")));
//                            log.debug("stampa mappa...");
//                            readIdMap.forEach(
//                                (idSorgente, idDuplicato) -> {
//                                   log.debug(String.format("id_sorgente: %s id_duplicato: %s", idSorgente, idDuplicato));
//                                });
//                            log.debug(String.format("id_fascicolo_padre duplicato = %s", idFascicoloPadre));
                            // se non sto copiando un fascicolo radice duplico anche i campi numero_fascicolo e nome_fascicolo
                            fascicoliRS.updateInt("numero_fascicolo", fascicoliRS.getInt("numero_fascicolo"));
                            fascicoliRS.updateString("nome_fascicolo", fascicoliRS.getString("nome_fascicolo"));
                        }
                        else { // se sono un fascicolo radice
                            // copio il nome, ma se finisce con _AnnoPrecedente, lo sostituisco con _AnnoCorrente
                            String nome = fascicoliRS.getString("nome_fascicolo");
                            if (nome.endsWith(String.format("_%d", currYear - 1)))
                                nome = nome.replace(String.format("_%d", currYear - 1), String.format("_%d", currYear));
                            fascicoliRS.updateString("nome_fascicolo", nome);

                            // inserisco il numero 0 (perché lo gennererò dopo con il servizio di numerazione,
                            fascicoliRS.updateInt("numero_fascicolo", 0);
                        }

                        // se non sono all'ultimo livello scrivo la mappatura id fascicolo sorgente -> id fascicolo duplicato (come spiegato precedentemente)
                        if (livello < MAX_LIVELLO_FASCICOLO)
                            writeIdMap.put(idFascicoloSorgente, idFascicoloDuplicato);

                        fascicoliRS.updateString("id_fascicolo_padre", idFascicoloPadre);

                        log.info("duplicazione campi...");
                        /*
                            per duplicare gli altri campi, estraggo tutti i nomi delle colonne dal risultato della query e li inserisco nel duplicato
                            leggendoli dal sorgente. Dalla duplicazione però escludo i campi presenti nell'array "COLUMN_TO_EXCLUDE"
                        */
                        ResultSetMetaData metaData = fascicoliRS.getMetaData();
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            String columnName = metaData.getColumnName(i);
                            //int columnType = metaData.getColumnType(i);
                            if (!Arrays.stream(COLUMN_TO_EXCLUDE).anyMatch(c -> c.equalsIgnoreCase(columnName))) {
                                //log.debug(String.format("processing column: %s of type: %s ",columnName, columnType));
                                fascicoliRS.updateObject(columnName, fascicoliRS.getObject(columnName));
                            }
                        }
                        
                        // inserisco la riga nel database e nel RecordSet
                        log.info("inserimento riga duplicata...");
                        fascicoliRS.insertRow();
                        log.info("inserimento OK");
                        
                        // rimetto il cursore al punto in cui era
                        fascicoliRS.moveToCurrentRow();
                        log.info("inserimento vicari...");
                        
                        // duplico anche i vicari
                        duplicateVicari(dbConn, idFascicoloSorgente, idFascicoloDuplicato);
                        log.info("inserimento vicari OK");
                        
                        // se sono al livello 1 (radice) numero il fascicolo con il servizio di numerazione
                        if (livello == 1) {
                            log.info("numerazione...");
                            String number = SetDocumentNumber.setNumber(dbConn, guidFascicoloDuplicato, sequenceName);
                            log.info(String.format("numero generato: %s", number));
                        }
                        fascicoliCont++;
                    }
                }
            }
        }
    }

    /**
     * Duplica i vicari
     * @param dbConn
     * @param idFascicoloSorgente
     * @param idFascicoloDuplicato
     * @throws SQLException 
     */
    private void duplicateVicari(Connection dbConn, String idFascicoloSorgente, String idFascicoloDuplicato) throws SQLException {
        
        String queryVicari = "select * from gd.fascicoli_gd_vicari where id_fascicolo = ?";
        try (PreparedStatement psVicari = dbConn.prepareStatement(queryVicari, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);) {
            psVicari.setString(1, idFascicoloSorgente);
            log.debug(String.format("eseguo la query: %s...", psVicari.toString()));
            ResultSet vicariRS = psVicari.executeQuery();
            if (vicariRS.last()) {
                int vicariNumber = vicariRS.getRow();
                log.debug(String.format("vicari tovati: ", vicariNumber));
                vicariRS.beforeFirst();
                int vicariCont = 0;
                while (vicariRS.next() && vicariCont < vicariNumber) {
                    log.debug(String.format("processing vicario row: %s --> %s", vicariRS.getString(1), vicariRS.getString(2)));
                    vicariRS.moveToInsertRow();
                    vicariRS.updateString("id_fascicolo", idFascicoloDuplicato);
                    vicariRS.updateString("id_utente", vicariRS.getString("id_utente"));
                    vicariRS.insertRow();
                    vicariCont++;
                    vicariRS.moveToCurrentRow();
                }
            }
            else {
                log.debug("non ci sono vicari");
            }
        }
    }
}