package it.bologna.ausl.bds_tools.ioda.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.bologna.ausl.bds_tools.SetDocumentNumber;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import static it.bologna.ausl.bds_tools.jobs.VersatoreParer.INDE_DOCUMENT_GUID_PARAM_NAME;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.Registro;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.ClassificazioneFascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicoli;
import it.bologna.ausl.ioda.iodaobjectlibrary.FascicoliPregressiMap;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.FascicoliSpecialiResearcher;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo.Creazione;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo.StatoFascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.Search;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import java.sql.Types;
import java.util.Collections;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class IodaFascicoliUtilities {

    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(IodaFascicolazioniUtilities.class);

    private static final String GENERATE_INDE_NUMBER_PARAM_NAME = "generateidnumber";
    private static final String INDE_DOCUMENT_ID_PARAM_NAME = "document_id";
    public static final String INDE_DOCUMENT_GUID_PARAM_NAME = "document_guid";
    public static final String CODICE_REGISTRO_FASCICOLO = "FASCICOLO";
    
    public static enum GetFascicoli {TIPO_FASCICOLO, SOLO_ITER, CODICE_FISCALE, ANCHE_CHIUSI, DAMMI_PERMESSI}


    private String gdDocTable;
    private String fascicoliTable;
    private String titoliTable;
    private String fascicoliGdDocTable;
    private String fascicoliGdVicari;
    private String utentiTable;
    private String prefixIds; // prefisso da anteporre agli id dei documenti che si inseriscono o che si ricercano (GdDoc, SottoDocumenti)
    private Search researcher;
    private Fascicolo fascicolo;
    HttpServletRequest request;

    private IodaFascicoliUtilities(Search r) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this.gdDocTable = ApplicationParams.getGdDocsTableName();
        this.fascicoliTable = ApplicationParams.getFascicoliTableName();
        this.titoliTable = ApplicationParams.getTitoliTableName();
        this.fascicoliGdDocTable = ApplicationParams.getFascicoliGdDocsTableName();
        this.utentiTable = ApplicationParams.getUtentiTableName();
        this.fascicoliGdVicari = ApplicationParams.getFascicoliGdVicariTableName();

        this.researcher = r;
    }

    public IodaFascicoliUtilities(HttpServletRequest request, Search r) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this(r);
        this.request = request;
//        this.researcher = r;
    }

    public IodaFascicoliUtilities(HttpServletRequest request, Fascicolo fascicolo) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this(null);
        this.request = request;
        this.fascicolo = fascicolo;
    }

    public Fascicoli getFascicoli(Connection dbConn, PreparedStatement ps) throws SQLException {
        Fascicoli fascicoli = getFascicoli(dbConn, ps, researcher.getSearchString(), researcher.getIdUtente(), researcher.getLimiteDiRicerca());

        return fascicoli;
    }

    private Fascicoli getFascicoli(Connection dbConn, PreparedStatement ps, String strToFind, String idUtente, int limit) throws SQLException {

        Fascicoli res = new Fascicoli();

        // La stringa cercata è una numerazione gerarchica? (Deve iniziare con un numero)
        boolean matcha = strToFind.matches("(\\d+-?)"
                + // n- , n
                "|(\\d+/\\d*)"
                + // n/ , n/n
                "|(\\d+\\-\\d+/?)"
                + // n-n/ , n-n
                "|(\\d+\\-\\d+/\\d+)"
                + // n-n/n
                "|(\\d+\\-\\d+\\-\\d*)"
                + // n-n- , n-n-n
                "|(\\d+\\-\\d+\\-\\d+/\\d*)");   // n-n-n/n , n-n-n/

        String whereCondition;

        if (matcha) {            
            // E' una numerazione gerarchica completa?
            if (strToFind.matches("(\\d+/\\d) | (\\d+\\-\\d+/\\d+) | (\\d+\\-\\d+\\-\\d+/\\d)")) {
                whereCondition = "where f.numerazione_gerarchica = '" + strToFind + "' "; // In questo caso uso l'uguale
            } else {
                whereCondition = "where f.numerazione_gerarchica like ('" + strToFind + "%')"; // In questo caso si vuole l'"inizia con"
            }
        } else {
            whereCondition = "where f.nome_fascicolo ilike ('%" + strToFind + "%')";
        }
        
//        Boolean soloIter = Boolean.parseBoolean((String) additionalData.get(GetFascicoli.SOLO_ITER.toString()));
//        Boolean ancheChiusi = Boolean.parseBoolean((String) additionalData.get(GetFascicoli.ANCHE_CHIUSI.toString()));
//        Boolean dammiPermessi = Boolean.parseBoolean((String) additionalData.get(GetFascicoli.DAMMI_PERMESSI.toString()));
//        
//        if (soloIter) {
//            whereCondition += " and id_iter is not null ";
//        }
//        
//        if (!ancheChiusi) {
//            whereCondition += " and f.stato_fascicolo != 'p' ";
//        }
                
        // Preparo la query
        String sqlText = "select distinct(f.id_fascicolo), f.id_livello_fascicolo, "
                + "CASE f.id_livello_fascicolo WHEN '2' THEN (select nome_fascicolo from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo) "
                + "WHEN '3' THEN (select nome_fascicolo from gd.fascicoligd where id_fascicolo = (select id_fascicolo_padre from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo)) "
                + "ELSE nome_fascicolo "
                + "END as nome_fascicolo_interfaccia, "
                + "f.numero_fascicolo, "
                + "f.nome_fascicolo,f.anno_fascicolo, f.id_utente_creazione, f.data_creazione, "
                + "f.numerazione_gerarchica, f.id_utente_responsabile, uResp.nome, uResp.cognome, "
                + "f.stato_fascicolo, f.id_utente_responsabile_proposto, f.id_fascicolo_padre, "
                + "f.id_titolo, f.speciale, t.codice_gerarchico || '' || t.codice_titolo || ' ' || t.titolo as titolo, "
                + "f.id_fascicolo_importato, f.data_chiusura, f.note_importazione, f.id_iter, "
                + "case when fv.id_fascicolo is null then 0 else -1 end as accesso "
                + "from "
                + "gd.fascicoligd f "
                + "left join procton.titoli t on t.id_titolo = f.id_titolo "
                + "left join gd.log_azioni_fascicolo laf on laf.id_fascicolo= f.id_fascicolo "
                + "join gd.fascicoli_modificabili fv on fv.id_fascicolo=f.id_fascicolo and fv.id_utente= ? "
                + "join procton.utenti uResp on uResp.id_utente=f.id_utente_responsabile "
                + "join procton.utenti uCrea on f.id_utente_creazione=uCrea.id_utente "
                + whereCondition + " "
                + "and f.numero_fascicolo != '0' and f.speciale != -1  "
                + "and f.stato_fascicolo != 'c' order by f.nome_fascicolo";

        // controllo se ci sono limiti da applicare
        if (limit != 0) {
            sqlText += " limit " + limit;
        }

        ps = dbConn.prepareStatement(sqlText);

        ps.setString(1, idUtente);
        log.debug("sql: " + ps.toString());

        ResultSet results = ps.executeQuery();

        while (results.next()) {
            int index = 1;
            String idFascicolo = results.getString(index++);
            String idLivelloFascicolo = results.getString(index++);
            String nomeFascicoloInterfaccia = results.getString(index++);
            int numeroFascicolo = results.getInt(index++);
            String nomeFascicolo = results.getString(index++);
            int annoFascicolo = results.getInt(index++);
            String idUtenteCreazione = results.getString(index++);
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dataCreazione = DateTime.parse(results.getString(index++), formatter);
            String numerazioneGerarchica = results.getString(index++);
            String idUtenteResponsabile = results.getString(index++);
            String nomeUtenteResponsabile = results.getString(index++);
            String cognomeUtenteResponsabile = results.getString(index++);
            String statoFascicolo = results.getString(index++);
            String idUtenteResponsabileProposto = results.getString(index++);
            String idFascicoloPadre = results.getString(index++);
            String idTitolo = results.getString(index++);
            int speciale = results.getInt(index++);
            String titolo = results.getString(index++);
            String idFascicoloImportato = results.getString(index++);
            DateTime dataChiusura = null;
            String tmp = results.getString(index++);
            if (tmp != null) {
                dataChiusura = DateTime.parse(tmp, formatter);
            }
            String noteImportazione = results.getString(index++);
            int idIter = results.getInt(index++);
            int accesso = results.getInt(index++);

            String descrizioneUtenteResponsabile = nomeUtenteResponsabile + " " + cognomeUtenteResponsabile;
            Fascicolo f = new Fascicolo();
            f.setCodiceFascicolo(numerazioneGerarchica);
            f.setNumeroFascicolo(numeroFascicolo);
            f.setNomeFascicolo(nomeFascicolo);
            f.setNomeFascicoloInterfaccia(nomeFascicoloInterfaccia);
            f.setIdUtenteResponsabile(idUtenteResponsabile);
            f.setDescrizioneUtenteResponsabile(descrizioneUtenteResponsabile);
            f.setDataCreazione(dataCreazione);
            f.setAnnoFascicolo(annoFascicolo);
            f.setIdLivelloFascicolo(idLivelloFascicolo);
            f.setIdUtenteCreazione(idUtenteCreazione);
            f.setStatoFascicolo(statoFascicolo);
            f.setSpeciale(speciale);
            f.setTitolo(titolo);
            f.setAccesso(accesso);
            f.setIdIter(idIter);
            f.setIdUtenteResponsabileProposto(idUtenteResponsabileProposto);
            f.setIdFascicoloImportato(idFascicoloImportato);
            f.setDataChiusura(dataChiusura);
            f.setNoteImportazione(noteImportazione);
            f.setClassificazioneFascicolo(getClassificazioneFascicolo(dbConn, ps, idFascicolo));
            
//            if (dammiPermessi) {
//                f.setPermessi(getPermessiDelFascicolo(dbConn, ps, idFascicolo));
//            }
            
            res.addFascicolo(f);
        }

        if (res.getSize() == 0) {
            res.createFascicoli();
        }

        return res;
    }
    
    
    public Fascicoli getFascicoliConPermessi(Connection dbConn, PreparedStatement ps, Map<String, Object> additionalData) throws SQLException {
        return getFascicoliConPermessi(dbConn, ps, researcher.getSearchString(), researcher.getLimiteDiRicerca(), additionalData);
    }
    
    private Fascicoli getFascicoliConPermessi(Connection dbConn, PreparedStatement ps, String strToFind, int limit, Map<String, Object> additionalData) throws SQLException {
        Fascicoli fascicoli = new Fascicoli();
        String whereCondition;
        
        // La stringa cercata è una numerazione gerarchica? (Deve iniziare con un numero)
        if (strToFind.matches("(\\d+-?)|(\\d+/\\d*)|(\\d+\\-\\d+/?)|(\\d+\\-\\d+/\\d+)|(\\d+\\-\\d+\\-\\d*)|(\\d+\\-\\d+\\-\\d+/\\d*)")) {            
            // E' una numerazione gerarchica completa?
            if (strToFind.matches("(\\d+/\\d{4})|(\\d+\\-\\d+/\\d{4})|(\\d+\\-\\d+\\-\\d+/\\d{4})")) {
                whereCondition = "where f.numerazione_gerarchica = '" + strToFind + "' "; // In questo caso uso l'uguale
            } else {
                whereCondition = "where f.numerazione_gerarchica like ('" + strToFind + "%')"; // In questo caso si vuole l'"inizia con"
            }
        } else {
            whereCondition = "where f.nome_fascicolo ilike ('%" + strToFind + "%')";
        }
        
        Boolean soloIter = Boolean.parseBoolean((String) additionalData.get(GetFascicoli.SOLO_ITER.toString()));
        Boolean ancheChiusi = Boolean.parseBoolean((String) additionalData.get(GetFascicoli.ANCHE_CHIUSI.toString()));
//        Boolean dammiPermessi = Boolean.parseBoolean((String) additionalData.get(GetFascicoli.DAMMI_PERMESSI.toString()));
        
        if (soloIter) {
            whereCondition += " and id_iter is not null ";
        }
        
        if (!ancheChiusi) {
            whereCondition += " and f.stato_fascicolo != 'p' and f.stato_fascicolo != 'c'";
        }
                
        // Preparo la query
        String sqlText = "select distinct f.id_fascicolo, f.id_livello_fascicolo, "
                + "f.numero_fascicolo, "
                + "f.nome_fascicolo,f.anno_fascicolo, f.id_utente_creazione, f.data_creazione, "
                + "f.numerazione_gerarchica, f.id_utente_responsabile, "
                + "f.stato_fascicolo, f.id_utente_responsabile_proposto, "
                + "f.speciale, "
                + "f.id_fascicolo_importato, f.data_chiusura, f.note_importazione, f.id_iter "
                + "from "
                + "gd.fascicoligd f "
                + whereCondition + " "
                + "and f.numero_fascicolo != '0' and f.speciale != -1  ";

        // controllo se ci sono limiti da applicare
        if (limit != 0) {
            sqlText += " limit " + limit;
        }

        ps = dbConn.prepareStatement(sqlText);
        log.debug("sql: " + ps.toString());

        ResultSet results = ps.executeQuery();

        while (results.next()) {
            int index = 1;
            String idFascicolo = results.getString(index++);
            String idLivelloFascicolo = results.getString(index++);
            int numeroFascicolo = results.getInt(index++);
            String nomeFascicolo = results.getString(index++);
            int annoFascicolo = results.getInt(index++);
            String idUtenteCreazione = results.getString(index++);
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dataCreazione = DateTime.parse(results.getString(index++), formatter);
            String numerazioneGerarchica = results.getString(index++);
            String idUtenteResponsabile = results.getString(index++);
            String statoFascicolo = results.getString(index++);
            String idUtenteResponsabileProposto = results.getString(index++);
            int speciale = results.getInt(index++);
            String idFascicoloImportato = results.getString(index++);
            DateTime dataChiusura = null;
            String tmp = results.getString(index++);
            if (tmp != null) {
                dataChiusura = DateTime.parse(tmp, formatter);
            }
            String noteImportazione = results.getString(index++);
            int idIter = results.getInt(index++);

            Fascicolo f = new Fascicolo();
            f.setCodiceFascicolo(numerazioneGerarchica);
            f.setNumeroFascicolo(numeroFascicolo);
            f.setNomeFascicolo(nomeFascicolo);
            f.setIdUtenteResponsabile(idUtenteResponsabile);
            f.setDataCreazione(dataCreazione);
            f.setAnnoFascicolo(annoFascicolo);
            f.setIdLivelloFascicolo(idLivelloFascicolo);
            f.setIdUtenteCreazione(idUtenteCreazione);
            f.setStatoFascicolo(statoFascicolo);
            f.setSpeciale(speciale);
            f.setIdIter(idIter);
            f.setIdUtenteResponsabileProposto(idUtenteResponsabileProposto);
            f.setIdFascicoloImportato(idFascicoloImportato);
            f.setDataChiusura(dataChiusura);
            f.setNoteImportazione(noteImportazione);
            f.setClassificazioneFascicolo(getClassificazioneFascicolo(dbConn, ps, idFascicolo));
            
            f.setPermessi(getPermessiDelFascicolo(dbConn, ps, idFascicolo));
            // TODO: Aggiungere i vicari, ma sono già ricavabili dai permessi in realtà..
            //f.setVicari(vicari);
            fascicoli.addFascicolo(f);
        }

        if (fascicoli.getSize() == 0) {
            fascicoli.createFascicoli();
        }

        return fascicoli;
    }
          
    public Map<String,String> getPermessiDelFascicolo(Connection dbConn, PreparedStatement ps, String idFascicolo) throws SQLException {
               
        String q = "with permessi as (\n" +
            "select u.cf, 'RESPONSABILE' as permesso\n" +
            "from gd.fascicoligd f\n" +
            "join procton.utenti u on u.id_utente = f.id_utente_responsabile\n" +
            "where f.id_fascicolo = ?\n" +
            "UNION\n" +
            "select u.cf, 'VICARIO' as permesso\n" +
            "from gd.fascicoligd f\n" +
            "join gd.fascicoli_gd_vicari v on f.id_fascicolo = v.id_fascicolo\n" +
            "join procton.utenti u on u.id_utente = v.id_utente\n" +
            "where f.id_fascicolo = ?\n" +
            "UNION\n" +
            "select u.cf, p.permesso\n" +
            "from gd.fascicoligd f\n" +
            "join procton_tools.oggetti o on o.id_oggetto = f.id_fascicolo\n" +
            "join procton_tools.permessi p on p.id_oggetto = o.id\n" +
            "join procton.utenti u on u.id_utente = p.id_utente\n" +
            "where f.id_fascicolo = ?\n" +
            "UNION ALL\n" +
            "select u.cf, p.permesso\n" +
            "from gd.fascicoligd f\n" +
            "join procton_tools.oggetti o on o.id_oggetto = f.id_fascicolo\n" +
            "join procton_tools.permessi p on p.id_oggetto = o.id\n" +
            "join procton.strutture s on s.id_struttura = p.scope\n" +
            "join procton.utenti u on u.id_struttura = s.id_struttura\n" +
            "where f.id_fascicolo = ? and p.id_utente is null\n" +
            "UNION ALL\n" +
            "select u.cf, p.permesso\n" +
            "from gd.fascicoligd f\n" +
            "join procton_tools.oggetti o on o.id_oggetto = f.id_fascicolo\n" +
            "join procton_tools.permessi p on p.id_oggetto = o.id\n" +
            "join procton.strutture s on s.id_struttura = p.scope\n" +
            "join procton.incarichi i on i.id_struttura = s.id_struttura\n" +
            "join procton.utenti u on u.id_utente = i.id_utente\n" +
            "where f.id_fascicolo = ? and p.id_utente is null\n" +
            "UNION ALL\n" +
            "select u.cf, p.permesso\n" +
            "from gd.fascicoligd f\n" +
            "join procton_tools.oggetti o on o.id_oggetto = f.id_fascicolo\n" +
            "join procton_tools.permessi p on p.id_oggetto = o.id\n" +
            "join procton.strutture s on s.id_struttura = p.scope\n" +
            "join procton.appartenenze_funzionali a on a.id_struttura = s.id_struttura\n" +
            "join procton.utenti u on u.id_utente = a.id_utente\n" +
            "where f.id_fascicolo = ? and p.id_utente is null\n" +
            ")\n" +
            "select distinct on (cf) cf, case\n" +
                    "	when permesso = '0' then 'NON_PERMESSO'\n" +
                    "	when permesso = '4' then 'VISUALIZZA'\n" +
                    "	when permesso = '6' then 'MODIFICA'\n" +
                    "	when permesso = '7' then 'CANCELLA'\n" +
                    "	else permesso end \n" +
            "from permessi\n" +
            "order by cf, case when permesso = 'RESPONSABILE' then 1 \n" +
            "	when permesso = 'VICARIO' then 2 \n" +
            "	when permesso = '7' then 3 \n" +
            "	when permesso = '6' then 4 \n" +
            "	when permesso = '4' then 5 \n" +
            "	when permesso = '0' then 6 \n" +
            "	end;";
        
        ps = dbConn.prepareStatement(q);
        
        int index = 1;
        ps.setString(index++, idFascicolo);
        ps.setString(index++, idFascicolo);
        ps.setString(index++, idFascicolo);
        ps.setString(index++, idFascicolo);
        ps.setString(index++, idFascicolo);
        ps.setString(index++, idFascicolo);

        log.debug("sql: " + ps.toString());

        ResultSet results = ps.executeQuery();
        Map<String,String> permessi = new HashMap<String,String>();
        
        while (results.next()) {
            index = 1;
            permessi.put(results.getString(index++), results.getString(index++));
        }
        
        return permessi;
    }

    public FascicoliPregressiMap getFascicoliPregressi(Connection dbConn, PreparedStatement ps) throws SQLException, JsonProcessingException {
        return getFascicoliPregressi(dbConn, ps, researcher.getSearchString(), researcher.getIdUtente());
    }

    private FascicoliPregressiMap getFascicoliPregressi(Connection dbConn, PreparedStatement ps, String str, String idUtente) throws SQLException {

        JSONArray jsonArray = (JSONArray) JSONValue.parse(str);
        List<String> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            list.add((String) jsonArray.get(i));
        }
        String idList = "'" + StringUtils.join(list, "','") + "'";

        FascicoliPregressiMap res = new FascicoliPregressiMap();

        String sqlText = "SELECT distinct(f.id_fascicolo), f.id_livello_fascicolo, "
                + "   CASE f.id_livello_fascicolo WHEN '2' THEN (select nome_fascicolo from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo) "
                + "		WHEN '3' THEN (select nome_fascicolo from gd.fascicoligd where id_fascicolo = (select id_fascicolo_padre from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo)) "
                + "		ELSE nome_fascicolo "
                + "        END as nome_fascicolo_interfaccia, "
                + "        f.numero_fascicolo, "
                + "        f.nome_fascicolo,f.anno_fascicolo, f.id_utente_creazione, f.data_creazione, "
                + "        f.numerazione_gerarchica, f.id_utente_responsabile, uResp.nome, uResp.cognome, "
                + "        f.stato_fascicolo, f.id_utente_responsabile_proposto, f.id_fascicolo_padre, "
                + "        f.id_titolo, f.speciale, t.codice_gerarchico || '' || t.codice_titolo || ' ' || t.titolo as titolo, "
                + "        f.id_fascicolo_importato, f.data_chiusura, f.note_importazione, f.guid_fascicolo "
                + "FROM gd.fascicoligd f "
                + "	LEFT join procton.titoli t ON t.id_titolo = f.id_titolo "
                + "	JOIN procton.utenti uResp ON uResp.id_utente=f.id_utente_responsabile "
                + "	JOIN procton.utenti uCrea ON f.id_utente_creazione=uCrea.id_utente "
                + "WHERE 1=1  "
                + "	AND f.guid_fascicolo in (" + idList + ") ";

        ps = dbConn.prepareStatement(sqlText);

        log.debug("Query: " + ps);

        ResultSet results = ps.executeQuery();

        while (results.next()) {
            int index = 1;
            String idFascicolo = results.getString(index++);
            String idLivelloFascicolo = results.getString(index++);
            String nomeFascicoloInterfaccia = results.getString(index++);
            int numeroFascicolo = results.getInt(index++);
            String nomeFascicolo = results.getString(index++);
            int annoFascicolo = results.getInt(index++);
            String idUtenteCreazione = results.getString(index++);

            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTimeFormatter formatterWithHHmmss = DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss");

            DateTime dataCreazione = DateTime.parse(results.getString(index++), formatter);
            String numerazioneGerarchica = results.getString(index++);
            String idUtenteResponsabile = results.getString(index++);
            String nomeUtenteResponsabile = results.getString(index++);
            String cognomeUtenteResponsabile = results.getString(index++);
            String statoFascicolo = results.getString(index++);
            String idUtenteResponsabileProposto = results.getString(index++);
            String idFascicoloPadre = results.getString(index++);
            String idTitolo = results.getString(index++);
            int speciale = results.getInt(index++);
            String titolo = results.getString(index++);
            String idFascicoloImportato = results.getString(index++);
            DateTime dataChiusura = null;
            Timestamp tmp = results.getTimestamp(index++);
            log.debug("Data chiusura fasciclo: " + tmp);
            if (tmp != null) {
                dataChiusura = new DateTime(tmp.getTime());
                log.debug("Data chiusura convertito: " + dataChiusura);
            }
            String noteImportazione = results.getString(index++);
            String guidFascicolo = results.getString(index++);

            String descrizioneUtenteResponsabile = nomeUtenteResponsabile + " " + cognomeUtenteResponsabile;
            Fascicolo f = new Fascicolo();
            f.setCodiceFascicolo(numerazioneGerarchica);
            f.setNumeroFascicolo(numeroFascicolo);
            f.setNomeFascicolo(nomeFascicolo);
            f.setNomeFascicoloInterfaccia(nomeFascicoloInterfaccia);
            f.setIdUtenteResponsabile(idUtenteResponsabile);
            f.setDescrizioneUtenteResponsabile(descrizioneUtenteResponsabile);
            f.setDataCreazione(dataCreazione);
            f.setAnnoFascicolo(annoFascicolo);
            f.setIdLivelloFascicolo(idLivelloFascicolo);
            f.setIdUtenteCreazione(idUtenteCreazione);
            f.setStatoFascicolo(statoFascicolo);
            f.setSpeciale(speciale);
            f.setTitolo(titolo);
            f.setIdUtenteResponsabileProposto(idUtenteResponsabileProposto);
            f.setIdFascicoloImportato(idFascicoloImportato);
            f.setDataChiusura(dataChiusura);
            f.setNoteImportazione(noteImportazione);
            f.setClassificazioneFascicolo(getClassificazioneFascicolo(dbConn, ps, idFascicolo));

            res.addFascicolo(guidFascicolo, f);
        }

        return res;
    }

    public Fascicoli getFascicoloSpeciale(Connection dbConn, PreparedStatement ps) throws SQLException {

        String sqlText = "SELECT id_fascicolo, numerazione_gerarchica, "
                + "numero_fascicolo, nome_fascicolo, anno_fascicolo, stato_fascicolo, "
                + "id_utente_creazione, id_utente_responsabile, data_creazione, id_livello_fascicolo, "
                + "id_fascicolo_importato, data_chiusura, note_importazione"
                + " FROM " + fascicoliTable
                + " WHERE anno_fascicolo = ? AND speciale = ?";
        ps = dbConn.prepareStatement(sqlText);

        FascicoliSpecialiResearcher specRes = (FascicoliSpecialiResearcher) researcher;
//        ps.setString(1, specRes.getNomeFascicolo());
        ps.setInt(1, specRes.getAnno());
        ps.setInt(2, -1);
        log.debug("sql: " + ps.toString());

        ResultSet results = ps.executeQuery();

        Fascicoli fascicoliSpeciali = new Fascicoli();

        while (results.next()) {

            String idFascicolo = results.getString(1);
            String numerazioneGerarchica = results.getString(2);
            int numeroFascicolo = results.getInt(3);
            String nomeFascicolo = results.getString(4);
            int annoFascicolo = results.getInt(5);
            String statoFascicolo = results.getString(6);
            String idUtenteCreazione = results.getString(7);
            String idUtenteResponsabile = results.getString(8);
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dataCreazione = DateTime.parse(results.getString(9), formatter);
            String idLivelloFascicolo = results.getString(10);
            String idFascicoloImportato = results.getString(11);
            DateTime dataChiusura = null;
            if (results.getString(12) != null) {
                dataChiusura = DateTime.parse(results.getString(12), formatter);
            }
            String noteImportazione = results.getString(13);

            Fascicolo f = new Fascicolo();
            f.setCodiceFascicolo(numerazioneGerarchica);
            f.setNumeroFascicolo(numeroFascicolo);
            f.setNomeFascicolo(nomeFascicolo);
            f.setIdUtenteResponsabile(idUtenteResponsabile);
            f.setDataCreazione(dataCreazione);
            f.setAnnoFascicolo(annoFascicolo);
            f.setIdUtenteCreazione(idUtenteCreazione);
            f.setStatoFascicolo(statoFascicolo);
            f.setIdLivelloFascicolo(idLivelloFascicolo);
            f.setIdFascicoloImportato(idFascicoloImportato);
            f.setDataChiusura(dataChiusura);
            f.setNoteImportazione(noteImportazione);

            f.setClassificazioneFascicolo(getClassificazioneFascicolo(dbConn, ps, idFascicolo));

            fascicoliSpeciali.addFascicolo(f);
        }

        return fascicoliSpeciali;
    }

    private ClassificazioneFascicolo getClassificazioneFascicolo(Connection dbConn, PreparedStatement ps, String idFascicolo) throws SQLException {

        ClassificazioneFascicolo classificazioneFascicolo = null;

        String sqlText
                = "SELECT t.codice_gerarchico, t.codice_titolo "
                + "FROM " + getFascicoliTable() + " f, " + getTitoliTable() + " t "
                + "WHERE id_fascicolo = ? "
                + "AND f.id_titolo = t.id_titolo ";

        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idFascicolo);
        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();

        String result;

        if (!res.next()) {
            log.error("SQLException: titolo non trovato");
            throw new SQLException("titolo non trovato");
        } else {
            result = res.getString(1) + res.getString(2);

            classificazioneFascicolo = new ClassificazioneFascicolo();

            // calcolo la gerarchia e setta il corrispettivo nome
            String[] parts = result.split("-");
            int dim = parts.length;

            while (dim > 0) {
                switch (dim) {
                    case 3:
                        String nomeSottoClasse = this.getTitolo(dbConn, ps, parts[0] + "-" + parts[1] + "-", parts[2]);
                        classificazioneFascicolo.setNomeSottoclasse(nomeSottoClasse);
                        classificazioneFascicolo.setCodiceSottoclasse(parts[0] + "-" + parts[1] + "-" + parts[2]);
                        break;

                    case 2:
                        String nomeClasse = this.getTitolo(dbConn, ps, parts[0] + "-", parts[1]);
                        classificazioneFascicolo.setNomeClasse(nomeClasse);
                        classificazioneFascicolo.setCodiceClasse(parts[0] + "-" + parts[1]);
                        break;

                    case 1:
                        String nomeCategoria = this.getTitolo(dbConn, ps, null, parts[0]);
                        classificazioneFascicolo.setNomeCategoria(nomeCategoria);
                        classificazioneFascicolo.setCodiceCategoria(parts[0]);
                        break;
                }
                dim--;
            }
        }
        return classificazioneFascicolo;
    }

    private String getTitolo(Connection dbConn, PreparedStatement ps, String codiceGerarchico, String codiceTitolo) throws SQLException {

        String sqlText = "SELECT titolo "
                + "FROM " + getTitoliTable() + " "
                + "WHERE codice_gerarchico = ? "
                + "AND codice_titolo = ? ";

        ps = dbConn.prepareStatement(sqlText);

        if (codiceGerarchico != null && !"".equals(codiceGerarchico)) {
            ps.setString(1, codiceGerarchico);
        } else {
            ps.setString(1, "");
        }

        ps.setString(2, codiceTitolo);

        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();

        if (!res.next()) {
            throw new SQLException("nessun titolo trovato");
        }
        return res.getString(1);
    }

    public String getNomeCognome(Connection dbConn, PreparedStatement ps, String idUtente) throws SQLException {

        String result = null;

        String sqlText
                = "SELECT nome, cognome "
                + "FROM " + getUtentiTable() + " "
                + "WHERE id_utente = ? ";

        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idUtente);
        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();

        if (!res.next()) {
            throw new SQLException("utente non trovato");
        } else {
            result = res.getString(1) + " " + res.getString(2);
        }

        return result;
    }

    public String getGdDocTable() {
        return gdDocTable;
    }

    public String getTitoliTable() {
        return titoliTable;
    }

    public String getFascicoliTable() {
        return fascicoliTable;
    }

    public String getFascicoliGdVicariTable() {
        return fascicoliGdVicari;
    }

    public String getFascicoliGdDocTable() {
        return fascicoliGdDocTable;
    }

    public String getUtentiTable() {
        return utentiTable;
    }

    public Fascicolo getFascicolo() {
        return fascicolo;
    }

    public String insertFascicolo(Connection dbConn, Map<String, Object> collectionData) throws IOException, MalformedURLException, SendHttpMessageException, SQLException, ServletException {

//        ottengo in ID di INDE
        String idInde = getIndeIdAndGuid().get(INDE_DOCUMENT_ID_PARAM_NAME);
        String guidInde = getIndeIdAndGuid().get(INDE_DOCUMENT_GUID_PARAM_NAME);

        String sqlText
                = "INSERT INTO " + getFascicoliTable() + "("
                + "id_fascicolo, nome_fascicolo, numero_fascicolo, anno_fascicolo, "
                + "stato_fascicolo, id_livello_fascicolo, id_struttura, "
                + "id_titolo, id_utente_responsabile, id_utente_creazione, "
                + "id_utente_responsabile_proposto, data_creazione, data_responsabilita, "
                + "eredita_permessi, "
                + "speciale, guid_fascicolo, "
                + "id_tipo_fascicolo, codice_fascicolo, "
                + "servizio_creazione, descrizione_iter, id_iter, visibile) "
                + "VALUES (?, ?, ?, ?, "
                + "?, ?, ?, "
                + "?, ?, ?, "
                + "?, ?, ?, "
                + "?, "
                + "?, ?, "
                + "?, ?, "
                + "?, ?, ?, ?)";
        
        // ======================= IFFONE ====
        // Faccio un iffone brutto perché se arrivo dall'Internauta devo ricavare id_utente tramite codice fiscale ed id_titolo tramite classificazione 
        String codiceFiscale = "";
        
        if (this.fascicolo.getIdUtenteCreazione() == null || this.fascicolo.getIdUtenteCreazione().equals("")) {
            codiceFiscale = this.fascicolo.getCodiceFiscaleUtenteCreazione();
            this.fascicolo.setIdUtenteCreazione(getIdUtenteDaCodiceFiscale(codiceFiscale, dbConn));
        }
        if (this.fascicolo.getIdUtenteResponsabile() == null || this.fascicolo.getIdUtenteResponsabile().equals("")) {
            if (codiceFiscale.equals(this.fascicolo.getCodiceFiscaleUtenteResponsabile())){
                this.fascicolo.setIdUtenteResponsabile(this.fascicolo.getIdUtenteCreazione());
            } else {
                this.fascicolo.setIdUtenteResponsabile(getIdUtenteDaCodiceFiscale(this.fascicolo.getCodiceFiscaleUtenteResponsabile(), dbConn));
            }
        }
        if (this.fascicolo.getIdUtenteResponsabileProposto()== null || this.fascicolo.getIdUtenteResponsabileProposto().equals("")) {
            if (codiceFiscale.equals(this.fascicolo.getCodiceFiscaleUtenteResponsabileProposto())){
                this.fascicolo.setIdUtenteResponsabileProposto(this.fascicolo.getIdUtenteCreazione());
            } else {
                this.fascicolo.setIdUtenteResponsabileProposto(getIdUtenteDaCodiceFiscale(this.fascicolo.getCodiceFiscaleUtenteResponsabileProposto(), dbConn));
            }
        }
        if (this.fascicolo.getIdStruttura() == null || this.fascicolo.getIdStruttura().equals("")) {
            if (this.fascicolo.getIdStrutturaInternauta() == null) {
                this.fascicolo.setIdStruttura(getIdStrutturaDaIdUtente(this.fascicolo.getIdUtenteResponsabile(), dbConn));
            } else {
                this.fascicolo.setIdStruttura(getIdStrutturaDaIdStrutturaInternauta(this.fascicolo.getIdStrutturaInternauta(), dbConn));
            }
        }
        if (this.fascicolo.getTitolo() == null || this.fascicolo.getTitolo().equals("")) {
            this.fascicolo.setTitolo(getIdTitoloDaClassificazione(this.fascicolo.getClassificazione(), dbConn));
        }
        
        // ================== FINE IFFONE ====

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;

            int year = DateTime.now().getYear();

            ps.setString(index++, idInde);
            ps.setString(index++, fascicolo.getNomeFascicolo());
            // metto il numero 0 come quando creo un fascicolo in bozza
            ps.setInt(index++, 0);
            ps.setInt(index++, year);
            // metto lo stato in bozza
            ps.setString(index++, StatoFascicolo.APERTO.getName());

            // imposto la creazione del fascicolo, per la creazione dei sottofascicoli
            // e inserti la si ripenserà se ci sarà bisogno
            ps.setString(index++, String.valueOf(1));

            ps.setString(index++, fascicolo.getIdStruttura());
            ps.setString(index++, fascicolo.getTitolo());
            ps.setString(index++, fascicolo.getIdUtenteResponsabile());
            ps.setString(index++, fascicolo.getIdUtenteCreazione());
            if (fascicolo.getIdUtenteResponsabileProposto() != null && !fascicolo.getIdUtenteResponsabileProposto().equals("")) {
                ps.setString(index++, fascicolo.getIdUtenteResponsabileProposto());
            } else {
                ps.setString(index++, null);
            }

            // data creazione
            ps.setDate(index++, java.sql.Date.valueOf(java.time.LocalDate.now()));
            // data responsabilità
            ps.setDate(index++, java.sql.Date.valueOf(java.time.LocalDate.now()));
            // eredita permessi: messo a -1 perchè ho visto che i più recenti sono stati imposttai a -1 e non a 1
            ps.setInt(index++, -1);

            ps.setInt(index++, fascicolo.getSpeciale());
            ps.setString(index++, guidInde);
            // idTipoFascicolo
            ps.setInt(index++, fascicolo.getIdTipoFascicolo());
            // codiceFascicolo
            ps.setString(index++, "babel_" + guidInde);
            // setto il servizio di creazione con valore API
            ps.setString(index++, Creazione.API.getName());
            ps.setString(index++, fascicolo.getDescrizioneIter());
            ps.setInt(index++, fascicolo.getIdIter());
            if (fascicolo.getVisibile() != null) {
                ps.setInt(index++, fascicolo.getVisibile());
            } else {
                ps.setInt(index++, -1);
            }
            
            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0) {
                throw new SQLException("Fascicolo non inserito");
            }

            insertVicari(dbConn, idInde, collectionData);

            registraDocumento(dbConn, ps, guidInde, CODICE_REGISTRO_FASCICOLO);

            return guidInde;
        }
    }
    
    private String getIdUtenteDaCodiceFiscale(String cf, Connection dbConn) throws SQLException {
        if(cf != null && !cf.equals("")) {
            String q = "SELECT id_utente FROM " + getUtentiTable() + " WHERE cf = ?";
            try (PreparedStatement ps = dbConn.prepareStatement(q)) {
                ps.setString(1, cf);
                log.debug("eseguo la query: " + q);
                ResultSet res = ps.executeQuery();
                if (!res.next()) {
                    throw new SQLException("utente non trovato");
                } else {
                    return res.getString(1);
                }
            }
        }
        
        return null;
    }
    
    private String getIdStrutturaDaIdStrutturaInternauta(Integer idStrutturaInternauta, Connection dbConn) throws SQLException {
        if (idStrutturaInternauta != null) {
            String q = "select id_struttura from ribaltone_utils.mappa_id_strutture where id_struttura_organigramma = ? limit 1";
            try (PreparedStatement ps = dbConn.prepareStatement(q)) {
                ps.setInt(1, idStrutturaInternauta);
                log.debug("eseguo la query: " + q);
                ResultSet res = ps.executeQuery();
                if (!res.next()) {
                    throw new SQLException("struttura non trovata");
                } else {
                    return res.getString(1);
                }
            }
        }
        
        return null;
    }
    
    private String getIdTitoloDaClassificazione(String classificazione, Connection dbConn) throws SQLException {
        if(classificazione != null && !classificazione.equals("")) {
            String q = "SELECT id_titolo FROM " + getTitoliTable() 
                    + " WHERE codice_titolo = ? and codice_gerarchico = ?";
//            String codiceTitolo = classificazione.substring(classificazione.lastIndexOf("-"));
//            String codiceGerarchico = classificazione.substring(0, classificazione.length()-2);
            try (PreparedStatement ps = dbConn.prepareStatement(q)) {
                ps.setString(1, classificazione.substring(classificazione.lastIndexOf("-") + 1));
                ps.setString(2, classificazione.substring(0, classificazione.length() - 2));
                log.debug("eseguo la query: " + q);
                ResultSet res = ps.executeQuery();
                if (!res.next()) {
                    throw new SQLException("titolo non trovato");
                } else {
                    return res.getString(1);
                }
            }
        }
        
        return null;
    }
    private String getIdStrutturaDaIdUtente(String idutente, Connection dbConn) throws SQLException {
        if(idutente != null && !idutente.equals("")) {
            String q = "SELECT id_struttura FROM " + getUtentiTable() 
                    + " WHERE id_utente = ?";
            try (PreparedStatement ps = dbConn.prepareStatement(q)) {
                ps.setString(1, idutente);
                log.debug("eseguo la query: " + q);
                ResultSet res = ps.executeQuery();
                if (!res.next()) {
                    throw new SQLException("utente non trovato");
                } else {
                    return res.getString(1);
                }
            }
        }
        
        return null;
    }

    public void insertVicari(Connection dbConn, String idFascicolo, Map<String, Object> collectionData) throws SQLException {
        Boolean traduciVicari = false;
        
        if (collectionData != null) {
            traduciVicari = (Boolean) collectionData.get(Fascicolo.OperazioniFascicolo.TRADUCI_VICARI.toString());
        }

        String sqlText
                = "INSERT INTO " + getFascicoliGdVicariTable() + "("
                + "id_fascicolo, id_utente) ";
        
        if (traduciVicari) {
            sqlText = sqlText + "VALUES (?, (SELECT id_utente from procton.utenti where cf = ?))";
        } else {
            sqlText = sqlText + "VALUES (?, ?)";
        }

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {

            for (String vicario : fascicolo.getVicari()) {
                ps.setString(1, idFascicolo);
                ps.setString(2, vicario);

                String query = ps.toString();
                log.debug("eseguo la query: " + query);
                int result = ps.executeUpdate();
                if (result <= 0) {
                    throw new SQLException("Fascicolo non inserito");
                }
            }
        }
    }

    /**
     * Restituisce una mappa contene id di INDE
     *
     * @return
     * @throws IOException
     * @throws MalformedURLException
     * @throws SendHttpMessageException
     */
    private Map<String, String> getIndeIdAndGuid() throws IOException, MalformedURLException, SendHttpMessageException {

        Map<String, String> result = new HashMap<>();

        // contruisce la mappa dei parametri per la servlet (va passato solo un parametro che è il numero di id da generare)
        Map<String, byte[]> params = new HashMap<>();

        // ottene un docid
        int idNumber = 1;
        params.put(GENERATE_INDE_NUMBER_PARAM_NAME, String.valueOf(idNumber).getBytes());

        String res = UtilityFunctions.sendHttpMessage(ApplicationParams.getGetIndeUrlServiceUri(), null, null, params, "POST", null);

        // la servlet torna un JsonObject che è una coppia (id, guid)
        JSONArray indeIdArray = (JSONArray) JSONValue.parse(res);
        JSONObject currentId = (JSONObject) indeIdArray.get(0);
        result.put(INDE_DOCUMENT_ID_PARAM_NAME, (String) currentId.get(INDE_DOCUMENT_ID_PARAM_NAME));
        result.put(INDE_DOCUMENT_GUID_PARAM_NAME, (String) currentId.get(INDE_DOCUMENT_GUID_PARAM_NAME));
        return result;
    }

    public String registraDocumento(Connection dbConn, PreparedStatement ps, String guid, String codiceRegistro) throws ServletException, SQLException {
        Registro r = Registro.getRegistro(codiceRegistro, dbConn);
        return SetDocumentNumber.setNumber(dbConn, guid, r.getSequenzaAssociata());
    }

    public Fascicolo getFascicolo(Connection dbConn, String guid) throws SQLException {

        Fascicolo fascicolo = new Fascicolo();
        String idFascicolo = null;

        String sqlText
                = "SELECT id_fascicolo, nome_fascicolo, numero_fascicolo, anno_fascicolo, "
                + "stato_fascicolo, id_livello_fascicolo, id_struttura, "
                + "id_titolo, id_utente_responsabile, id_utente_creazione, "
                + "id_utente_responsabile_proposto, data_creazione, "
                + "numerazione_gerarchica, "
                + "speciale, "
                + "id_tipo_fascicolo, codice_fascicolo, data_registrazione, "
                + "servizio_creazione, descrizione_iter "
                + "FROM " + getFascicoliTable() + " "
                + "WHERE guid_fascicolo = ? ";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, guid);

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = ps.executeQuery();

            while (result.next()) {
                idFascicolo = result.getString("id_fascicolo");
                fascicolo.setNomeFascicolo(result.getString("nome_fascicolo"));
                fascicolo.setNumeroFascicolo(result.getInt("numero_fascicolo"));
                fascicolo.setAnnoFascicolo(result.getInt("anno_fascicolo"));
                fascicolo.setStatoFascicolo(result.getString("stato_fascicolo"));
                fascicolo.setIdLivelloFascicolo(result.getString("id_livello_fascicolo"));
                fascicolo.setIdStruttura(result.getString("id_struttura"));
                fascicolo.setTitolo(result.getString("id_titolo"));
                fascicolo.setIdUtenteResponsabile(result.getString("id_utente_responsabile"));
                fascicolo.setIdUtenteCreazione(result.getString("id_utente_creazione"));
                fascicolo.setIdUtenteResponsabileProposto(result.getString("id_utente_responsabile_proposto"));

                Timestamp dataCreazione = result.getTimestamp("data_creazione");
                if (dataCreazione != null) {
                    fascicolo.setDataCreazione(new DateTime(dataCreazione.getTime()));
                }

                fascicolo.setNumerazioneGerarchica(result.getString("numerazione_gerarchica"));
                fascicolo.setSpeciale(result.getInt("speciale"));
                fascicolo.setIdTipoFascicolo(result.getInt("id_tipo_fascicolo"));
                fascicolo.setCodiceFascicolo(result.getString("codice_fascicolo"));
                Timestamp dataRegistrazione = result.getTimestamp("data_registrazione");
                if (dataRegistrazione != null) {
                    fascicolo.setDataRegistrazione(new DateTime(dataRegistrazione.getTime()));
                }

                fascicolo.setServizioCreazione(result.getString("servizio_creazione"));
                fascicolo.setDescrizioneIter(result.getString("descrizione_iter"));
            }
        }

        String sqlTextVicari
                = "SELECT id_utente "
                + "FROM " + getFascicoliGdVicariTable() + " "
                + "WHERE id_fascicolo = ? ";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlTextVicari)) {
            ps.setString(1, idFascicolo);

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = ps.executeQuery();

            while (result.next()) {
                fascicolo.addVicario(result.getString("id_utente"));
            }
        }
        return fascicolo;
    }
    
    /**
     * Questa funzione deve ritorna la lista dei fascicoli di livello 1 su cui l'utente ha un permesso almeno di modifica.
     * Si può passare il parametro tipo fascicolo per aggiungerlo come where condition.
     * @param dbConn
     * @param ps
     * @param idUtente
     * @param tipoFascicolo
     * @param conIter
     * @return
     * @throws SQLException 
     */
    public Fascicoli getFascicoliUtente(Connection dbConn, PreparedStatement ps, Map<String, Object> additionalData) throws SQLException {
        
        Fascicoli res = new Fascicoli();
        String idUtente = researcher.getIdUtente();
        Integer tipoFascicolo = Integer.parseInt((String) additionalData.get(GetFascicoli.TIPO_FASCICOLO.toString()));
        Boolean soloIter = Boolean.parseBoolean((String) additionalData.get(GetFascicoli.SOLO_ITER.toString()));
        
        if (idUtente == null || idUtente.equals("")) {
            String q = "select id_utente, count(*) over (partition by 1) total_rows from procton.utenti where cf = ?";
            ps = dbConn.prepareStatement(q);
            ps.setString(1, (String) additionalData.get(GetFascicoli.CODICE_FISCALE.toString()));
            log.debug("sql: " + ps.toString());
            ResultSet result = ps.executeQuery();
            if (result.next()) {
                if (result.getInt("total_rows") > 1) {
                    throw new SQLException("Trovate più righe per questo utente");                 
                }
                idUtente = result.getString("id_utente"); 
            } else {
                throw new SQLException("Utente non trovato");
            }
        }

        // Preparo la query
        String sqlText = "with strutture_utente as(\n" +
            "	select id_struttura\n" +
            "	from procton.utenti\n" +
            "	where id_utente = ?\n" +
            "	union all\n" +
            "	select id_struttura\n" +
            "	from procton.appartenenze_funzionali\n" +
            "	where id_utente = ?\n" +
            ")\n" +
            "select f.numerazione_gerarchica, f.nome_fascicolo, f.numero_fascicolo, f.anno_fascicolo, f.stato_fascicolo, f.id_livello_fascicolo, \n" +
            "   f.id_struttura, f.id_utente_responsabile, f.id_utente_creazione, f.id_utente_responsabile_proposto, f.data_creazione, f.numerazione_gerarchica, f.id_tipo_fascicolo, \n" +
            "   f.data_registrazione::date, f.descrizione_iter, f.id_iter\n" +
            "from gd.fascicoligd f  \n" +
            "left join gd.fascicoli_gd_vicari fv on f.id_fascicolo = fv.id_fascicolo\n" +
            "left join procton_tools.oggetti o on o.id_oggetto = f.id_fascicolo\n" +
            "left join procton_tools.permessi p on p.id_oggetto = o.id\n" +
            "where   \n" +
            "f.numero_fascicolo != '0' and f.speciale != -1 and f.stato_fascicolo != 'p' and f.stato_fascicolo != 'c' and id_livello_fascicolo = '1'\n" +
            "and (id_utente_responsabile = ? or fv.id_utente = ? \n" +
            "	or (p.id_utente = ? and p.permesso::bpchar >= 6::character(1)) \n" +
            "	or (p.scope in (select * from strutture_utente) and p.id_utente is null and p.permesso::bpchar >= 6::character(1)))";
        
        // Controllo se devo aggiungere il filtro per tipoFascicolo
        if (tipoFascicolo != 0) {
            sqlText += " and id_tipo_fascicolo = " + tipoFascicolo;
        }
        
        // Controllo se il campo iter deve essere NOT NULL
        if (soloIter) {
            sqlText += " and id_iter is not null";
        }

        ps = dbConn.prepareStatement(sqlText);
        
        int index = 1;
        ps.setString(index++, idUtente);
        ps.setString(index++, idUtente);
        ps.setString(index++, idUtente);
        ps.setString(index++, idUtente);
        ps.setString(index++, idUtente);
        log.debug("sql: " + ps.toString());

        ResultSet results = ps.executeQuery();
        
        while (results.next()) {
            Fascicolo f = new Fascicolo();
            DateTimeFormatter formatDate = DateTimeFormat.forPattern("yyyy-MM-dd");

            f.setCodiceFascicolo(results.getString("numerazione_gerarchica")); // E' la numerazione gerarchica (anche se ce l'abbiamo più sotto)
            f.setNomeFascicolo(results.getString("nome_fascicolo"));
            f.setNumeroFascicolo(results.getInt("numero_fascicolo"));
            f.setAnnoFascicolo(results.getInt("anno_fascicolo"));
            f.setStatoFascicolo(results.getString("stato_fascicolo"));
            f.setIdLivelloFascicolo(results.getString("id_livello_fascicolo"));
            f.setIdStruttura(results.getString("id_struttura"));
            f.setIdUtenteResponsabile(results.getString("id_utente_responsabile"));
            f.setIdUtenteCreazione(results.getString("id_utente_creazione"));
            f.setIdUtenteResponsabileProposto(results.getString("id_utente_responsabile_proposto"));
            f.setDataCreazione(DateTime.parse(results.getString("data_creazione"), formatDate));
            f.setNumerazioneGerarchica(results.getString("numerazione_gerarchica"));
            f.setIdTipoFascicolo(results.getInt("id_tipo_fascicolo"));
            f.setDataRegistrazione(DateTime.parse(results.getString("data_registrazione"), formatDate));
            f.setDescrizioneIter(results.getString("descrizione_iter"));
            f.setIdIter(results.getInt("id_iter"));

            res.addFascicolo(f);
        }

        if (res.getSize() == 0) {
            res.createFascicoli();
        }
        
        return res;
    }
    
    public String getIdUtenteByCFFromAdditionalData(Connection dbConn, String cf) throws SQLException{
        String idUtente;
        String q = "select id_utente, count(*) over (partition by 1) total_rows from procton.utenti where cf = ?";
            try(PreparedStatement ps = dbConn.prepareStatement(q)){
                ps.setString(1, cf);
                log.debug("*************       -->       sql: " + ps.toString());
                ResultSet result = ps.executeQuery();
                if (result.next()) {
                    if (result.getInt("total_rows") > 1) {
                        throw new SQLException("Trovate più righe per questo utente");                 
                    }
                    idUtente = result.getString("id_utente"); 
                } else {
                    throw new SQLException("Utente non trovato");
                }
            }
        return idUtente;
    }
    
    public List<String> getIdUtentiByCodiciFiscali(Connection dbConn, PreparedStatement ps, List<String> cfs) throws SQLException{
        log.info("Prendo gli id utente dai codici fiscali...");
        log.debug("CODICI = " + cfs);
        String q = "select id_utente, count(*) over (partition by 1) total_rows from procton.utenti where cf IN (";
        q += String.join("", Collections.nCopies(cfs.size(), "?,"));
        q = q.substring(0, q.length() - 1) + ")";
        List<String> idUtenti = new ArrayList<>();
        ps = dbConn.prepareStatement(q);
 
        int index = 1;
        for (String cf: cfs) {
            ps.setString(index++, cf);
        }
       log.info("eseguo la query...");
        ResultSet result = ps.executeQuery();
        if (result.next()) {
            if (result.getInt("total_rows") != cfs.size()) {
                throw new SQLException("Previsti " + cfs.size() + " risultati. Trovati: "
                    + result.getInt("total_rows"));                 
            }
            do {
                idUtenti.add(result.getString("id_utente"));
            } while (result.next());
            log.info("Id utenti dei vicari = " + idUtenti.toString());
        } else {
            throw new SQLException("Nessun utente trovato.");
        }
        
        return idUtenti;
    }
    
    public Boolean updateFascicolo(Connection dbConn, PreparedStatement ps, HashMap additionalData) throws SQLException {
       boolean deletePrecedente = false;
       boolean vengoDaGipi = false;
       Integer idIterPrecedente = null;
       
       
       if (additionalData != null) {
           if ((Boolean) additionalData.get(Fascicolo.OperazioniFascicolo.DELETE_ITER_PRECEDENTE.toString()) != null){
               deletePrecedente = true;
           } 
           
           if ((Boolean) additionalData.get(Fascicolo.OperazioniFascicolo.PROVENIENZA_GIPI.toString()) != null){
               vengoDaGipi = true;
           } 
           
           if ((Integer) additionalData.get(Fascicolo.OperazioniFascicolo.ID_ITER_PRECEDENTE.toString()) != null){
               idIterPrecedente = (Integer) additionalData.get(Fascicolo.OperazioniFascicolo.ID_ITER_PRECEDENTE.toString());
           } 
        }
        
        String sqlText
                = "UPDATE " + getFascicoliTable() + " SET "
                + "nome_fascicolo = coalesce(?, nome_fascicolo), "
                + "numero_fascicolo = coalesce(?, numero_fascicolo), "
                + "anno_fascicolo = coalesce(?, anno_fascicolo), "
                + "stato_fascicolo = coalesce(?, stato_fascicolo), "
                + "id_livello_fascicolo = coalesce(?, id_livello_fascicolo), "
                + "id_struttura = coalesce(?, id_struttura), "
                + "id_titolo = coalesce(?, id_titolo), "
                + "id_utente_responsabile = coalesce(?, id_utente_responsabile), "
                + "id_utente_creazione = coalesce(?, id_utente_creazione), "
                + "id_utente_responsabile_proposto = coalesce(?, id_utente_responsabile_proposto), "
                + "descrizione_iter = coalesce(?, descrizione_iter), "
                + (deletePrecedente ? "id_padre_catena_fascicolare = ? " : "id_padre_catena_fascicolare = coalesce(?, id_padre_catena_fascicolare) ") 
                + (vengoDaGipi ? "WHERE id_iter = ? " : "WHERE numerazione_gerarchica = ? ") ;
        
        ps = dbConn.prepareStatement(sqlText);
        
        int index = 1;

        setStringOrNull(ps, index++, fascicolo.getNomeFascicolo());
        setIntOrNull(ps, index++, fascicolo.getNumeroFascicolo());
        setIntOrNull(ps, index++, fascicolo.getAnnoFascicolo());
        setStringOrNull(ps, index++, fascicolo.getStatoFascicolo());
        setStringOrNull(ps, index++, fascicolo.getIdLivelloFascicolo());
        setStringOrNull(ps, index++, fascicolo.getIdStruttura());
        setStringOrNull(ps, index++, fascicolo.getTitolo());         
        setUtenteOrNull(ps, index++, fascicolo.getIdUtenteResponsabile(), dbConn);         
        setUtenteOrNull(ps, index++, fascicolo.getIdUtenteCreazione(),dbConn);         
        setUtenteOrNull(ps, index++, fascicolo.getIdUtenteResponsabileProposto(), dbConn);         
        setStringOrNull(ps, index++, fascicolo.getDescrizioneIter());
        setStringOrNull(ps, index++, getIdFascicoloByIter(dbConn, idIterPrecedente));
        if (vengoDaGipi){
            ps.setInt(index++, fascicolo.getIdIter());
        } else {
            ps.setString(index++, fascicolo.getCodiceFascicolo());
        }
        
        

        String query = ps.toString();
        log.info("eseguo la query di update del fascicolo con numerazione gerarchica = "
                + fascicolo.getCodiceFascicolo());
        int result = ps.executeUpdate();
        if (result == 0){
            throw new SQLException("Fascicolo non trovato");
        }
        
        if (!vengoDaGipi){
        
            log.info("Prendo l'id fascicolo che servirà per aggiornare i vicari se richiesto ...");
            String idFascicolo = getIdFascicolo(dbConn, ps);

            List<String> vicari = fascicolo.getVicari();
            if (vicari != null && !vicari.isEmpty()) {
                log.info("Aggiorno i vicari...");
                List<String> idUtenteVicari = getIdUtentiByCodiciFiscali(dbConn, ps, vicari);
                deleteVicari(dbConn, ps, idFascicolo);  // Elimino tutti i vicari
                insertVicari(dbConn, ps, idFascicolo, idUtenteVicari);// E li riaggiungo
            }
        }
        return true;
    }
    
    public void setIntOrNull(PreparedStatement pstmt, int column, int value) throws SQLException {
        if (value != 0) {
            pstmt.setInt(column, value);
        } else {
            pstmt.setNull(column, Types.INTEGER);
        }
    }
    
    public void setStringOrNull(PreparedStatement pstmt, int column, String value) throws SQLException {
        if (value != null) {
            pstmt.setString(column, value);
        } else {
            pstmt.setNull(column, Types.VARCHAR);
        }
    }
    
    public void setUtenteOrNull(PreparedStatement pstmt, int column, String value, Connection dbConn) throws SQLException {
        if (value != null) {
            value = getIdUtenteByCFFromAdditionalData(dbConn, value);
            pstmt.setString(column, value);
        } else {
            pstmt.setNull(column, Types.VARCHAR);
        }
    }
    
    public void deleteVicari(Connection dbConn, PreparedStatement ps, String idFascicolo) throws SQLException {
        String sqlText
                = "DELETE FROM " + getFascicoliGdVicariTable() + " "
                + "WHERE id_fascicolo = ?";
        ps = dbConn.prepareStatement(sqlText);

        ps.setString(1, idFascicolo);

        String query = ps.toString();
        log.info("eseguo la query di delete dei vicari ...");
        int result = ps.executeUpdate();
    }
    
    public void insertVicari(Connection dbConn, PreparedStatement ps, String idFascicolo, List<String> nuoviVicari) throws SQLException {
        String sqlText
                = "INSERT INTO " + getFascicoliGdVicariTable() + "("
                + "id_fascicolo, id_utente) "
                + "VALUES ";
        sqlText += String.join("", Collections.nCopies(nuoviVicari.size(), "( ?, ?),"));
        sqlText = sqlText.substring(0, sqlText.length() - 1);
        ps = dbConn.prepareStatement(sqlText);

        int index = 1;
        for (String vicario: nuoviVicari) {
            ps.setString(index++, idFascicolo);
            ps.setString(index++, vicario);// DA CAMBIARE
        }
        String query = ps.toString();
        log.info("eseguo la query di inserimento Vicari ...");
        int result = ps.executeUpdate();
        
    }
    
    public String getIdFascicolo(Connection dbConn, PreparedStatement ps) throws SQLException {
         String sqlText
                = "SELECT id_fascicolo from " + getFascicoliTable() + " "
                + "WHERE numerazione_gerarchica = ?";
        String res = "";
        ps = dbConn.prepareStatement(sqlText);

        int index = 1;
        ps.setString(index++, fascicolo.getCodiceFascicolo());

        String query = ps.toString();
        log.info("eseguo la query per trovare l'id ...");
        ResultSet result = ps.executeQuery();
        if (result.next()) {
            res = result.getString(1);
            log.info("Id fascicolo trovato = " + res);
        } else {
            throw new SQLException("ID fascicolo non trovato");
        }
        return res;
    }
    // ricerca un generico permesso sul fascicolo (responsabilità, vicario, permessi)
    public boolean doesUserHaveAnyPermissionOnThisFascicolo(Connection dbConn, HashMap additionalData) throws SQLException {
        boolean hasSomePermission = false;
        String numerazioneGerarchica;
        String idUtente = researcher.getIdUtente();
        if (idUtente == null || idUtente.equals("")) 
            idUtente  = getIdUtenteByCFFromAdditionalData(dbConn, (String) additionalData.get("user").toString());
        
        
        numerazioneGerarchica = additionalData.get("ng").toString();
        
        hasSomePermission = isUserResponsabileFascicolo(dbConn, numerazioneGerarchica, idUtente) 
                || isUserVicario(dbConn, numerazioneGerarchica, idUtente) 
                || hasUserPermissionOnFascicolo(dbConn, numerazioneGerarchica, idUtente);
        
        log.debug("hasSomePermission: " + hasSomePermission);
        return hasSomePermission;
    }
    
    // cerca se l'utente ha permessi sul fascicolo (ricerca per numerazione_gerarchica)
//    public boolean hasUserPermissionOnFascicolo(Connection dbConn, HashMap additionalData) throws SQLException {
//        String idUtente = researcher.getIdUtente();
//        boolean trovato = false;
//        if (idUtente == null || idUtente.equals("")) 
//            idUtente  = getIdUtenteByCFFromAdditionalData(dbConn, (String) additionalData.get("user").toString());
//        
//        // Scelgo tutti i permessi facendo join tra permessi, oggetti, fascicoli
//        // "WHERE" il fascicolo ha quella numerazione gerarchica 
//        // e o l'utente loggato ha i permessi o ce li ha la sua struttura
//        // NB: solo la struttura di appartenenza dell'azienda
//        String sql = "select f.numerazione_gerarchica, p.scope as struttura, v.id_utente as utente_vedente, p.id_utente as utente_permesso from procton_tools.permessi "
//                + "join procton_tools.oggetti o on o.id = p.id_oggetto "
//                + "join gd.fascicoligd f on f.id_fascicolo = o.id_oggetto "
//                + "join gd.fascicoli_visibili v on v.id_fascicolo = f.id_fascicolo "
//                + "where f.numerazione_gerarchica = ? and (p.id_utente = ? or v.id_utente = ? "
//                + "and p.permesso::bpchar>=4::character(1) "
//                + "and o.tipo_oggetto = 4 "; // 4 è il tipo oggetto fascicolo
//        
//              
//        try (PreparedStatement ps = dbConn.prepareStatement(sql)) {
//            ps.setString(1, additionalData.get("ng").toString()); // numerazione gerarchica
//            ps.setString(2, idUtente);
//            ps.setString(3, idUtente);
//        
//            String psToString = ps.toString();
//            System.out.println("ESEGUO " + psToString);
//            log.debug("****SQL****: " + ps.toString());
//            ResultSet results;
//                results = ps.executeQuery();
//                if(results.next())
//                    trovato = true; 
//                else
//                    System.out.println("L'utente non ha permessi sul fascicolo");
//        } 
//        catch (Exception ex) {
//            throw new SQLException("Problemi nel trovare i permessi dell'utente  " + ex);
//        }
//                
//        log.debug("trovatp: " + trovato);
//        return trovato;
//        
//    }
    
    
    public boolean hasUserPermissionOnFascicolo(Connection dbConn, String numerazioneGerarchica, String user) throws SQLException{
        boolean hasPermission = false;
        
        String sql = "with strutture_utente as (\n" +
                "	select id_struttura\n" +
                "	from procton.utenti\n" +
                "	where id_utente = ?\n" +
                "	union all\n" +
                "	select id_struttura\n" +
                "	from procton.appartenenze_funzionali\n" +
                "	where id_utente = ?\n" +
                ")\n" +
                "select 1\n" +
                "from gd.fascicoligd f\n" +
                "join procton_tools.oggetti o on o.id_oggetto = f.id_fascicolo\n" +
                "join procton_tools.permessi p on p.id_oggetto = o.id\n" +
                "where numerazione_gerarchica = ? and\n" +
                "(\n" +
                "	(p.id_utente = ? or (p.scope in (select * from strutture_utente) and p.id_utente is null))\n" +
                "	and p.permesso::bpchar>=4::character(1)\n" +
                ")";
        
        try (PreparedStatement ps = dbConn.prepareStatement(sql)) {
            ps.setString(1, user);
            ps.setString(2, user);
            ps.setString(3, numerazioneGerarchica);
            ps.setString(4, user);
            
            String psToString = ps.toString();
            System.out.println("ESEGUO " + psToString);
            log.debug("****SQL****: " + ps.toString());
            ResultSet results;
                results = ps.executeQuery();
                if(results.next())
                    hasPermission = true;
                else
                    log.debug("L'utente non ha permessi sul fascicolo");
        }
        catch (Exception ex) {
            throw new SQLException("Problemi nel trovare i permessi dell'utente  " + ex);
        }
        
        log.debug("trovato: " + hasPermission);
        return hasPermission;
    }
    
    // cerca se l'utente è vicario per il fascicolo (ricerca per numerazione_gerarchica)
    public boolean isUserVicario(Connection dbConn, HashMap additionalData) throws SQLException {
        String idUtente = researcher.getIdUtente();
        boolean trovato = false;
        if (idUtente == null || idUtente.equals("")) 
            idUtente  = getIdUtenteByCFFromAdditionalData(dbConn, (String) additionalData.get("user").toString());  
        
        // faccio una join tra fascicoligd e fascicoli_gd_vicari e cerco se c'è l'utente
        String sql = "select f.numerazione_gerarchica, v.id_utente "
                + "from  gd.fascicoligd f "
                + "join gd.fascicoli_gd_vicari v on f.id_fascicolo = v.id_fascicolo "
                + "where f.numerazione_gerarchica = ? and v.id_utente = ? ";
        
              
        try (PreparedStatement ps = dbConn.prepareStatement(sql)) {
            // numerazione gerarchica
            ps.setString(1, additionalData.get("ng").toString());
            ps.setString(2, idUtente);
        
            String psToString = ps.toString();
            System.out.println("ESEGUO " + psToString);
            log.debug("****SQL****: " + ps.toString());
            ResultSet results;
                results = ps.executeQuery();
                if(results.next())
                    trovato = true;
                else
                    System.out.println("L'utente non è vicario del fascicolo");
        } 
        catch (Exception ex) {
            throw new SQLException("Problemi nel cercare l'utente tra i vicari " + ex);
        }
                
        log.debug("trovatp: " + trovato);
        return trovato;
        
    }
    
    
    // stesso metodo con firma diversa
    public boolean isUserVicario(Connection dbConn, String numerazioneGerarchica, String user) throws SQLException{
        boolean hasPermission = false;
        
        String sql = "select f.numerazione_gerarchica, v.id_utente "
                + "from  gd.fascicoligd f "
                + "join gd.fascicoli_gd_vicari v on f.id_fascicolo = v.id_fascicolo "
                + "where f.numerazione_gerarchica = ? and v.id_utente = ? ";
        
              
        try (PreparedStatement ps = dbConn.prepareStatement(sql)) {
            // numerazione gerarchica
            ps.setString(1, numerazioneGerarchica);
            ps.setString(2, user);
        
            String psToString = ps.toString();
            System.out.println("ESEGUO " + psToString);
            log.debug("****SQL****: " + ps.toString());
            ResultSet results;
                results = ps.executeQuery();
                if(results.next())
                    hasPermission = true; 
                else
                    System.out.println("L'utente non è vicario del fascicolo");
        } 
        catch (Exception ex) {
            throw new SQLException("Problemi nel cercare l'utente tra i vicari " + ex);
        }
                
        log.debug("hasPermission: " + hasPermission);
        
        return hasPermission;
    }
    
    // cerca se l'utente è id_responsabile_fascicolo sulla riga del fascicolo (ricerca per numerazione_gerarchica)
    public boolean isUserResponsabileFascicolo(Connection dbConn, HashMap additionalData) throws SQLException {
        String idUtente = researcher.getIdUtente();
        boolean trovato = false;
        if (idUtente == null || idUtente.equals("")) 
            idUtente  = getIdUtenteByCFFromAdditionalData(dbConn, (String) additionalData.get("user").toString());
        
        // query secca where numerazione_gerarchica = X and id_utente responsabile = Y
        String sql = "select f.numerazione_gerarchica, f.id_utente_responsabile "
                + "from  gd.fascicoligd f "
                + "where f.numerazione_gerarchica = ? and f.id_utente_responsabile = ? ";
        
              
        try (PreparedStatement ps = dbConn.prepareStatement(sql)) {
            // numerazione gerarchica
            ps.setString(1, additionalData.get("ng").toString());
            ps.setString(2, idUtente);
        
            String psToString = ps.toString();
            System.out.println("ESEGUO " + psToString);
            log.debug("****SQL****: " + ps.toString());
            ResultSet results;
                results = ps.executeQuery();
                if(results.next())
                    trovato = true; 
                else
                    System.out.println("L'utente non è responsabile del fascicolo");
        } 
        catch (Exception ex) {
            throw new SQLException("Problemi nel cercare l'utente tra i vicari " + ex);
        }
                
        log.debug("trovatp: " + trovato);
        return trovato;
    }
    
    // stesso metodo con firma diversa
    public boolean isUserResponsabileFascicolo(Connection dbConn, String numerazioneGerarchica, String user) throws SQLException{
        boolean hasPermission = false;
        
        String sql = "select f.numerazione_gerarchica, f.id_utente_responsabile "
                + "from  gd.fascicoligd f "
                + "where f.numerazione_gerarchica = ? and f.id_utente_responsabile = ? ";
        
              
        try (PreparedStatement ps = dbConn.prepareStatement(sql)) {
            // numerazione gerarchica
            ps.setString(1, numerazioneGerarchica);
            ps.setString(2, user);

            String psToString = ps.toString();
            System.out.println("ESEGUO " + psToString);
            log.debug("****SQL****: " + ps.toString());
            ResultSet results;
                results = ps.executeQuery();
                if(results.next())
                    hasPermission = true;
                else
                    System.out.println("L'utente non è responsabile del fascicolo");
        } 
        catch (Exception ex) {
            throw new SQLException("Problemi nel cercare l'utente tra i vicari " + ex);
        }
                
        log.debug("hasPermission: " + hasPermission);
        
        return hasPermission;
    }
    
    public String getIdFascicoloByIter(Connection dbConn, Integer idIter) throws SQLException{

        String res = null;
        
        if (idIter != null){
           String sql = "select f.id_fascicolo "
               + "from  gd.fascicoligd f "
               + "where id_iter = ? ";

              
            try (PreparedStatement ps = dbConn.prepareStatement(sql)) {

                ps.setInt(1, idIter);

                ResultSet result;
                    result = ps.executeQuery();
                    if(result.next())
                        res = result.getString(1);
                    else
                        System.out.println("non esiste fascicolo con id_iter: " + idIter);
            } 
            catch (Exception ex) {
                throw new SQLException("ricerca fascicolo con id_iter non andata a buon fine" + ex);
            } 
        }
        return res;
    }
}
