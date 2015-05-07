package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.utils.Base64Coder;
import it.bologna.ausl.bds_tools.utils.SupportedFile;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.mail.MessagingException;
import javax.mail.internet.MimeUtility;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author utente
 */
public class Test {
    
    public static void main(String[] args) throws JsonProcessingException, IOException, MalformedURLException, SendHttpMessageException, UnsupportedEncodingException, MessagingException {
        File file_1 = new File("c:/tmp/test_trep.pdf");        
        File file_2 = new File("c:/tmp/Haven.S05E06.720p.HDTV.X264-DIMENSION.mkv");
 
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
       // DateTime jodatime = dtf.parseDateTime("2015-05-03T12:12:00");
         DateTime jodatime = DateTime.now();
        
        System.out.println(jodatime.toString());
        GdDoc gddoc = new GdDoc("idGdDoc1", "tipoGdDoc", "nome", "gdm", "inserimento", true,  jodatime,true, "PG", jodatime, "55", "sa", false, false, false);
        
        List firmatari = new ArrayList();
        firmatari.add("firmatario");
        
//        DataInputStream datais1 = new DataInputStream(new FileInputStream(file_1));
//        byte[] file1Bytes = new byte[(int)file_1.length()];
//        datais1.readFully(file1Bytes);
//        IodaFile file = new IodaFile(file_1.getName(), Base64Coder.encodeLines(file1Bytes));
//        
        SottoDocumento sd = new SottoDocumento(
                "IdSott1", 
                "topoSott", 
                Document.DocumentOperationType.INSERT, 
                "cod_sott_1", 
                "tipologia1", 
                null, 
                null, 
                true, 
                "nomesott1", 
                null, 
                null, 
                null, 
                "file_orig_part", 
                null, 
                null, 
                null);
        SottoDocumento sd2 = new SottoDocumento(
                "idSott2", 
                "tipoSott", 
                Document.DocumentOperationType.INSERT,
                "cod_sott_2",
                "tipologia2", 
                SottoDocumento.TipoFirma.DIGITALE, 
                firmatari, 
                true, 
                "nomesott2", 
                null, 
                null, 
                null, 
                null, 
                "file_firm_part", 
                null, 
                null);
//        System.out.println(sd2.getTipoFirma().toString());

        List<SottoDocumento> ls = new ArrayList<SottoDocumento>();
        ls.add(sd);
        ls.add(sd2);
        
//        gddoc.setSottoDocumenti(ls);
        
        Fascicolo fasc = new Fascicolo("babel_EA3A8C9D-D7A9-0500-10A2-2B524AFEC63F", Document.DocumentOperationType.INSERT);
        ArrayList<Fascicolo> fascicoli = new ArrayList<Fascicolo>();
        fascicoli.add(fasc);
//        gddoc.setFascicoli(fascicoli);


        IodaRequest req = new IodaRequest("test", "test", gddoc);  
        String json = req.getJSONString();
        System.out.println(json);
//       System.exit(0);
        IodaRequest reqParsata = IodaRequest.parse(json);
        System.out.println(reqParsata.getJSONString());
        
//        GdDoc gdDocParsato = GdDoc.parse(json, GdDoc.class);
//        System.out.println(gdDocParsato.getJSONString());
        
        Map<String, byte[]> params = new HashMap<String, byte[]>();
        params.put("dati", json.getBytes());
        String res = UtilityFunctions.sendHttpMessage("http://localhost:8082/bds_tools/ioda/api/document/delete", null, null, params, "POST", "application/json");
        System.out.println(res);
        System.exit(0);
       //String bao = "{\"idOggettoOrigine\":\"ciao\",\"tipoOggettoOrigine\":\"tipo\",\"nome\":\"nome\",\"record\":true,\"dataUltimaModifica\":\"2015-04-01T17:26:12\",\"visibile\":true,\"codice_registro\":\"PG\",\"dataRegistrazione\":\"2015-04-10T17:28:12+0200\",\"numeroRegistrazione\":\"55\",\"xmlSpecificoParer\":\"sa\",\"forzaConservazione\":false,\"forzaAccettazione\":false,\"forzaCollegamento\":false}";



        String docString = "{\"idOggettoOrigine\":\"idGdDoc1\",\"tipoOggettoOrigine\":\"tipoGdDoc\",\"nome\":\"nome\",\"record\":true,\"dataUltimaModifica\":\"2015-04-15T14:14:27+0200\",\"visibile\":true,\"codiceRegistro\":\"PG\",\"dataRegistrazione\":\"2015-04-15T14:14:27+0200\",\"numeroRegistrazione\":\"55\",\"xmlSpecificoParer\":\"sa\",\"forzaConservazione\":false,\"forzaAccettazione\":false,\"forzaCollegamento\":false,\"fascicoli\":[{\"anno\":f,\"tipoOperazione\":\"insert\"}],\"sottoDocumenti\":[{\"idOggettoOrigine\":\"IdSott1\",\"tipoOggettoOrigine\":\"topoSott\",\"tipoOperazione\":\"update\",\"tipo\":\"tipologia1\",\"tipoFirma\":\"digitale\",\"firmatari\":[\"firmatario\"],\"mimeType\":\"application/pdf\",\"principale\":true,\"nome\":\"nome\",\"uuid_mongo_pdf\":\"uuid1\",\"uuid_mongo_firmato\":\"uuid2\",\"uuid_mongo_originale\":\"uuid3\",\"fileOriginale\":{\"filename\":\"gdm.ok\",\"value\":\"ciaociaociao\"},\"fileFirmato\":{\"filename\":\"gdm.ok\",\"value\":\"ciaociaociao\"}},{\"idOggettoOrigine\":\"idSott2\",\"tipoOggettoOrigine\":\"tipoSott\",\"tipoOperazione\":\"insert\",\"tipo\":\"tipologia2\",\"tipoFirma\":\"autografa\",\"firmatari\":[\"firmatario\"],\"mimeType\":\"PDF\",\"principale\":true,\"nome\":\"nome\",\"uuid_mongo_pdf\":\"uu1\",\"uuid_mongo_firmato\":\"uufirmato\",\"uuid_mongo_originale\":\"orig\",\"fileOriginale\":{\"filename\":\"gdm.ok\",\"value\":\"ciaociaociao\"},\"fileFirmato\":{\"filename\":\"gdm.ok\",\"value\":\"ciaociaociao\"}}]}";
        GdDoc gdDocParsatoFromString = GdDoc.parse(docString, GdDoc.class);
        System.out.println(gdDocParsatoFromString.getJSONString());

        
    }
    
}
