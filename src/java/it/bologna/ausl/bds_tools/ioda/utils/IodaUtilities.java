package it.bologna.ausl.bds_tools.ioda.utils;

import it.bologna.ausl.bds_tools.exceptions.RequestException;
import it.bologna.ausl.bds_tools.ioda.api.InsertGdDoc;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.IodaRequestDescriptor;
import java.io.IOException;
import java.io.InputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author gdm
 */
public class IodaUtilities {
    private static final Logger log = LogManager.getLogger(IodaUtilities.class);
 
    public static final String REQUEST_DESCRIPTOR_PART_NAME = "request_descriptor";
    
    public static IodaRequestDescriptor extractIodaRequest(HttpServletRequest request) throws RequestException, IOException, ServletException {
        InputStream iodaRequestIs = null;
        try {
            Part iodaRequestPart = request.getPart(REQUEST_DESCRIPTOR_PART_NAME);
            if (iodaRequestPart != null)
                iodaRequestIs = iodaRequestPart.getInputStream();
            else 
                throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, "parte " + REQUEST_DESCRIPTOR_PART_NAME + " non trovata");

            IodaRequestDescriptor iodaRequest;
            if (iodaRequestIs != null) {
                try {
                    iodaRequest = IodaRequestDescriptor.parse(iodaRequestIs);
                    return iodaRequest;
                }
                catch (Exception ex) {
                    throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, "errore nel parsing di " + REQUEST_DESCRIPTOR_PART_NAME + ": ", ex);
                }
            }
            else
                throw new RequestException(HttpServletResponse.SC_BAD_REQUEST, "contenuto di " + REQUEST_DESCRIPTOR_PART_NAME + " vuoto");
        }
        finally {
            IOUtils.closeQuietly(iodaRequestIs);
        }
    }
}
