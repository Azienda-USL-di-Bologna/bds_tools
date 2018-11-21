package it.bologna.ausl.bds_tools.jobs.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.logging.log4j.LogManager;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author spritz
 */
public class VersatoreParerUtils {
    
    private static final Logger log = LogManager.getLogger(VersatoreParerUtils.class);
    
    public String getNomeDocumentoInErrore(String xmlVersato, String errorMessage) {
        String res = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder;
        Document doc = null;
        String regexComponente = "^.*-(\\w*)-(\\d+):(\\d):(\\d+):";
        String regexUnitaDocumentaria = "^.* (\\w*)-(\\d+)-(\\d+):";
        
        if (xmlVersato != null && !xmlVersato.equals("") && errorMessage != null && !errorMessage.equals("")) {
            log.info("getNomeDocumentoInErrore: xmlVersato e errorMessage settati");
            try {
                builder = factory.newDocumentBuilder();
                InputStream documentIS = new ByteArrayInputStream(xmlVersato.getBytes(StandardCharsets.UTF_8));
                doc = builder.parse(documentIS);
                
                Pattern pattern = Pattern.compile(regexComponente);
                Matcher matcher = pattern.matcher(errorMessage);
                
                String tipoDocumento = null;
                Integer numeroDocumento = null;
                Integer numeroOrdineComponente = null;
                
                while (matcher.find()) {
                    tipoDocumento = matcher.group(1);
                    numeroDocumento = Integer.valueOf(matcher.group(2));
                    numeroOrdineComponente = Integer.valueOf(matcher.group(4));
                }
                
                if (tipoDocumento == null && numeroDocumento == null && numeroOrdineComponente == null) {
                    // prova il secondo parsing
                    pattern = Pattern.compile(regexUnitaDocumentaria);
                    matcher = pattern.matcher(errorMessage);
                    
                    while (matcher.find()) {
                        tipoDocumento = matcher.group(1);
                        numeroDocumento = Integer.valueOf(matcher.group(2));
                        numeroOrdineComponente = Integer.valueOf(matcher.group(4));
                    }
                }
                    
                // Crea oggetto XPathFactory
                XPathFactory xpathFactory = XPathFactory.newInstance();

                // crea oggetto XPath
                XPath xpath = xpathFactory.newXPath();

                // numero ordine struttura viene omesso perchè attualmente si ha solo ordine struttura = 1
                log.info("calcolo del nome del componente in errore");
                res = getNomeComponente(doc, xpath, tipoDocumento, numeroDocumento, numeroOrdineComponente);
            } catch (Exception ex) {
                log.error("errore nel parsing: ", ex);
            }
        }
        return res;
    }
    
    private static String getNomeComponente(Document doc, XPath xpath, String tipoDocumento, Integer numeroDocumento, Integer numeroOrdineComponente) {
        XPathExpression exprNomeComponente;
        XPathExpression exprOrdinePresentazione;
        String nomeComponente = null;
        Integer ordinePresentazione;
        try {
            
            switch (tipoDocumento) {
                
                case "ALLEGATO":
                    exprOrdinePresentazione = xpath.compile("/UnitaDocumentaria/Allegati/Allegato[" + numeroDocumento + "]/StrutturaOriginale/Componenti/Componente/OrdinePresentazione");
                    ordinePresentazione = ((Double) exprOrdinePresentazione.evaluate(doc, XPathConstants.NUMBER)).intValue();

                    // controllo di sicurezza per vedere se anche l'oridne di presentazione corrisponde
                    if (Objects.equals(ordinePresentazione, numeroOrdineComponente)) {
                        exprNomeComponente = xpath.compile("/UnitaDocumentaria/Allegati/Allegato[" + numeroDocumento + "]/ProfiloDocumento/Descrizione/text()");
                        nomeComponente = (String) exprNomeComponente.evaluate(doc, XPathConstants.STRING);
                    }
                    break;
                
                case "ANNOTAZIONE":
                    exprOrdinePresentazione = xpath.compile("/UnitaDocumentaria/Annotazioni/Annotazione[" + numeroDocumento + "]/StrutturaOriginale/Componenti/Componente/OrdinePresentazione");
                    ordinePresentazione = ((Double) exprOrdinePresentazione.evaluate(doc, XPathConstants.NUMBER)).intValue();

                    // controllo di sicurezza per vedere se anche l'oridne di presentazione corrisponde
                    if (Objects.equals(ordinePresentazione, numeroOrdineComponente)) {
                        exprNomeComponente = xpath.compile("/UnitaDocumentaria/Annotazioni/Annotazione[" + numeroDocumento + "]/StrutturaOriginale/Componenti/Componente/NomeComponente/text()");
                        nomeComponente = (String) exprNomeComponente.evaluate(doc, XPathConstants.STRING);
                    }
                    break;
                case "PRINCIPALE":
                    exprOrdinePresentazione = xpath.compile("/UnitaDocumentaria/DocumentoPrincipale[" + numeroDocumento + "]/StrutturaOriginale/Componenti/Componente/OrdinePresentazione");
                    ordinePresentazione = ((Double) exprOrdinePresentazione.evaluate(doc, XPathConstants.NUMBER)).intValue();

                    // controllo di sicurezza per vedere se anche l'oridne di presentazione corrisponde
                    if (Objects.equals(ordinePresentazione, numeroOrdineComponente)) {
                        exprNomeComponente = xpath.compile("/UnitaDocumentaria/DocumentoPrincipale[" + numeroDocumento + "]/StrutturaOriginale/Componenti/Componente/NomeComponente/text()");
                        nomeComponente = (String) exprNomeComponente.evaluate(doc, XPathConstants.STRING);
                    }
                    break;
                
                case "ANNESSO":
                    exprOrdinePresentazione = xpath.compile("/UnitaDocumentaria/Annessi/Annesso[" + numeroDocumento + "]/StrutturaOriginale/Componenti/Componente/OrdinePresentazione");
                    ordinePresentazione = ((Double) exprOrdinePresentazione.evaluate(doc, XPathConstants.NUMBER)).intValue();

                    // controllo di sicurezza per vedere se anche l'oridne di presentazione corrisponde
                    if (Objects.equals(ordinePresentazione, numeroOrdineComponente)) {
                        exprNomeComponente = xpath.compile("/UnitaDocumentaria/Annessi/Annesso[" + numeroDocumento + "]/StrutturaOriginale/Componenti/Componente/NomeComponente/text()");
                        nomeComponente = (String) exprNomeComponente.evaluate(doc, XPathConstants.STRING);
                    }
                    break;
                
                default:
                    nomeComponente = "";
                    break;
            }
        } catch (XPathExpressionException ex) {
            log.error("errore di XPathExpression: ", ex);
            nomeComponente = null;
        }
        
        return nomeComponente;
    }
    
    
    
}
