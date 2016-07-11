/*
 * Copyright 2014 EMBL-EBI.
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
import static io.netty.handler.codec.http.HttpResponseStatus.SEE_OTHER;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadService;
import uk.ac.embl.ebi.ega.downloadservice.utils.MyCacheEntry;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

// user could pass ?md5=md5 - then we'd know for sure of the download was successful

public class ResultService extends ServiceTemplate implements Service {
/*
    @Override
    public JSONObject handle(String id, Map<String, String> parameters, FullHttpRequest request) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        Resty r = new Resty();
        try {
            String url = "http://localhost:"+EgaSecureDownloadService.getServer("res")+"/ega/reencryption/v1/results/" + id;
            JSONResource json1 = r.json(url);
            JSONObject jobj = (JSONObject) json1.get("response");
            JSONArray jsonarr = (JSONArray)jobj.get("result");
            for (int i=0; i<jsonarr.length(); i++) {
                System.out.println("  " + i + " : " + jsonarr.getString(i));
            }

            for (int i=0; i<jsonarr.length(); i++)
                System.out.println("  " + jsonarr.getString(i));
            String[] result = new String[]{jsonarr.getString(0)};
            
            json.put("header", responseHeader(OK)); // Header Section of the response
            json.put("response", responseSection(result));            
        } catch (Exception ex) {
            Logger.getLogger(ResultService.class.getName()).log(Level.SEVERE, null, ex);
        }

        return json;
    }
*/    
    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, String ip, EgaSecureDownloadService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        String entry = (id!=null&&id.size()>0)?id.get(0):"";
        MyCacheEntry mce = EgaSecureDownloadService.getEntry(entry);
  
        if (mce!=null) {
            try {
                String submitted_md5 = "";
                try {
                    submitted_md5 = parameters.get("md5");
                } catch (Throwable t) {;}

                String[] result = new String[]{mce.getMD5(), String.valueOf(mce.getSize())};
                String text = mce.getMD5() + " __ " + submitted_md5;
                EgaSecureDownloadService.logEvent(mce.getIP(), "*", text, "MD5 Verification Request", entry, "*");
                EgaSecureDownloadService.log(mce.getIP() + "*" + text + "MD5 Verification Request" + entry + "*");

                // If md5s don't match (TODO: update download_log) and return proper code!!
                if (submitted_md5!=null && submitted_md5.length() > 0) {
                    if (mce.getMD5() != null && mce.getMD5().length()>0 && mce.getMD5().equalsIgnoreCase(submitted_md5))
                        json.put("header", responseHeader(OK)); // Header Section of the response                        
                    else
                        json.put("header", responseHeader(SEE_OTHER)); // Header Section of the response
                } else
                    json.put("header", responseHeader(OK)); // Header Section of the response
                
                json.put("response", responseSection(result));            
            } catch (Exception ex) {
                Logger.getLogger(ResultService.class.getName()).log(Level.SEVERE, null, ex);
                try {
                    json.put("header", responseHeader(SEE_OTHER, ex.getLocalizedMessage()));
                    json.put("response", responseSection(null));            
                } catch (JSONException ex1) {;}
            }
        } else {
            EgaSecureDownloadService.log("mce not found for '" + entry + "'");
            try {
                json.put("header", responseHeader(SEE_OTHER, "Server couldn't find MD5 entry for " + entry));
                json.put("response", responseSection(null));            
            } catch (JSONException ex1) {;}
        }

        return json;
    }
}
