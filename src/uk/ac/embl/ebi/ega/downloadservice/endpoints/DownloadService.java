/*
 * Copyright 2015 EMBL-EBI.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.embl.ebi.ega.downloadservice.endpoints;

import io.netty.handler.codec.http.FullHttpRequest;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadService;
import static uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadService.restCall;
import uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadServiceHandler;
import uk.ac.embl.ebi.ega.downloadservice.utils.EgaFile;
import uk.ac.embl.ebi.ega.downloadservice.utils.EgaTicket;
import uk.ac.embl.ebi.ega.downloadservice.utils.RestyTimeOutOption;
import us.monoid.json.JSONArray;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;
import us.monoid.web.JSONResource;
import us.monoid.web.Resty;

/*
 * Pre-Process a Download Request:
 *  - Retrieve Ticket information from Access
 *  - Potentially amend ticket information for CRG (local paths, etc)
 *  - Set up Re-Encryption Stream on RES based on ticket information
 *  - Return RES URL for Re-Encrypted Data Stream
 *
 *  Download itself happens in main handler class
 */
public class DownloadService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, String ip, EgaSecureDownloadService ref) {
        JSONObject json = null; // Start out with common JSON Object

        String org = null;
        try {
            org = parameters.containsKey("org")?parameters.get("org"):null;
        } catch (Throwable th) {;}
        if (org==null)
            org = EgaSecureDownloadService.getOrganization();
        
        String id_ = (id!=null&&id.size()>0)?id.get(0):"";
        try {
EgaSecureDownloadService.log("ID: " + id_ + " ip: " + ip);
            
            // /downloads/{downloadticket}
            EgaTicket[] ticket = null;
            if (id != null && id_.length()>0) {
                // get ticket information
                ticket = EgaSecureDownloadService.getTicket(id_, ip);
            }

            EgaTicket ticket_ = (ticket!=null && ticket.length>0)?ticket[0]:null; // There will be 0 or 1
            if (ticket_ != null) {
                // Location-specific: If not EGA, get local file path from Catalog based on EGA File ID
                String fp = ticket_.getFileName();
                if (EgaSecureDownloadService.getOrganization().equalsIgnoreCase("LOCAL-EGA")) {
                    fp = getLocalPath(ticket_.getFileID());
                    ticket_.setFileName(fp);
                }
                if (fp!=null && fp.length()>0) ticket_.setFileName(fp);

                String[] resource = prepareResource(ticket_, org);

                json = new JSONObject();
                json.put("header", responseHeader(OK)); // Header Section of the response
                json.put("response", responseSection(resource));
            } else {
EgaSecureDownloadService.log("Error getting Ticket ("+id_+"): Ticket is null");
                System.out.println("Error getting Ticket ("+id_+"): Ticket is null");
            }
        } catch (JSONException ex) {
            System.out.println("Error getting Ticket ("+id_+"): " + ex.getLocalizedMessage());
            EgaSecureDownloadService.log("Error getting Ticket ("+id_+"): " + ex.getLocalizedMessage());
        }
        
        return json;
    }
    
    // POST to RES for Resource -- new RES
    private static String[] prepareResource(EgaTicket ticket_, String org) {
        String[] resource = null;
        EgaTicket ticket = ticket_;
        
        System.out.println("PrepareResource!");
        try {
            Resty r = new Resty(new RestyTimeOutOption(10000, 8000));
            
            JSONObject json = new JSONObject();
            // New RES
            String fp = ticket.getFileName();
            json.put("filepath", fp); // File - full path or relative path for Cleversafe 
            String pathtype = "";
            if (fp.toLowerCase().contains("virtual"))
                pathtype = "Virtual";
            else if (EgaSecureDownloadService.getOrganization().equalsIgnoreCase("EBI-EGA"))
                pathtype = "Cleversafe";
            else
                pathtype = "Absolute";
            json.put("pathtype", pathtype); // Type of Path: "Cleversafe" or "Absolute" or "Virtual"
            String oformat = "AES256";
            json.put("originformat", oformat); // Origin format: "AES256", "AES128", "SymmetricGPG", "PublicGPG", "Plain"
            String format = "AES128";
            json.put("destinationformat", format); // Destination format: "AES256", "AES128", "SymmetricGPG", "PublicGPG_{org}", "Plain"
            json.put("originkey", ""); // Decryption Key - blank in most cases; determined by format
            json.put("destinationkey", ticket.getEncryptionKey()); // (Re)Encryption Key (user supplied, or blank if PublicGPG/Plain is chosen)
            
            String server = EgaSecureDownloadService.getServer("res");

            String url = server+"/files";
            //JSONResource json1 = r.json(url, form( data("filerequest", content(json)) ));
            JSONResource json1 = restCall(r, url, json, "filerequest");
            //if (json1==null) json1 = r.json(url, form( data("filerequest", content(json)) ));
                
            System.out.println("Resource Prepared? " + (json1!=null));

            JSONObject jobj_ = (JSONObject) json1.get("response");
            JSONArray jsonarr_ = (JSONArray)jobj_.get("result");
            assert(jsonarr_.length()>0);
            
            String resource_uuid = jsonarr_.getString(0);
            String ticket_0_ = ticket.getUser(); // User (email)
            String  ticket_1_ = ticket.getFileID(); // EGAF File ID
            String  ticket_2_ = ""; // ?? Not used anyway...
            String  ticket_3_ = format; // ReEncryption Format (AES128)
            String  ticket_4_ = ticket.getFileName(); // File Name
            
            resource = new String[]{resource_uuid,server, ticket_0_, ticket_1_, ticket_2_, ticket_3_, ticket_4_};

        } catch (Exception ex) {
            Logger.getLogger(EgaSecureDownloadServiceHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return resource;
    }
    
    // Pass in EGAF... ID, get back asolute path of the file... from database now
    private String getLocalPath(String id) {
        
        EgaFile f = EgaSecureDownloadService.getFile(id);

        String path = "";
        System.out.println("Getting Path for " + id);
        path = f.getFileIndex();
        
        return path;
    }
}
