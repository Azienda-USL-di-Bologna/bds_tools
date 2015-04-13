/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.ioda;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author utente
 */
public class Test {
    
    public static void main(String[] args) throws JsonProcessingException, IOException {
        
        
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
       // DateTime jodatime = dtf.parseDateTime("2015-05-03T12:12:00");
         DateTime jodatime = DateTime.now();
        
        System.out.println(jodatime.toString());
        GdDoc gddoc = new GdDoc("ciao", "tipo", "nome", true,  jodatime,true, "PG", jodatime, "55", "sa", false, false, false);
        
        List l = new ArrayList();
        l.add("firmatario");
        SottoDocumento sd = new SottoDocumento("tipologia", SottoDocumento.TipoFirma.DIGITALE, l, "PDF", true, "abc123", "nome", "uu1", "uufirmato", "orig", "file1", "file2");
        SottoDocumento sd2 = new SottoDocumento("tipologia2", SottoDocumento.TipoFirma.AUTOGRAFA, l, "PDF", true, "1111", "nome", "uu1", "uufirmato", "orig", "file1", "file2");
        
        List<SottoDocumento> ls = new ArrayList<SottoDocumento>();
        ls.add(sd);
        ls.add(sd2);
        
        gddoc.setSottoDocumenti(ls);
        
        String json = gddoc.getJSONString();
        System.out.println(json);
        
       //String bao = "{\"idOggettoOrigine\":\"ciao\",\"tipoOggettoOrigine\":\"tipo\",\"nome\":\"nome\",\"record\":true,\"dataUltimaModifica\":\"2015-04-01T17:26:12\",\"visibile\":true,\"codice_registro\":\"PG\",\"dataRegistrazione\":\"2015-04-10T17:28:12+0200\",\"numeroRegistrazione\":\"55\",\"xmlSpecificoParer\":\"sa\",\"forzaConservazione\":false,\"forzaAccettazione\":false,\"forzaCollegamento\":false}";
        
        GdDoc gdDocParsato = GdDoc.parse(json, GdDoc.class);
        System.out.println(gdDocParsato.getJSONString());
        
////        List l = new ArrayList();
////        l.add("firmatario");
////        SottoDocumento sd = new SottoDocumento("tipologia", SottoDocumento.TipoFirma.DIGITALE, l, "PDF", true, "abc123", "nome", "uu1", "uufirmato", "orig", "file1", "file2");
////        
////        String jsonSd = sd.getJSONString();
////        System.out.println(jsonSd);
////        
////        SottoDocumento sdParsato = SottoDocumento.parse(jsonSd, SottoDocumento.class);
////        System.out.println(sdParsato.getJSONString());
    }
    
}
