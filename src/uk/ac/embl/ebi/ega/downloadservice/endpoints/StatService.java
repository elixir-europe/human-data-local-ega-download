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
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadService;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

/**
 *
 * @author asenf
 */
public class StatService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, String ip, EgaSecureDownloadService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        try {
            String function = (id!=null&&id.size()>0)?id.get(0):"";
            String[] result = null; // Holds JDBC Database Query results
            if (function.equalsIgnoreCase("load")) { // CPU Load
                result = new String[]{String.valueOf(EgaSecureDownloadService.getSystemCpuLoad())};
            } else if (function.equalsIgnoreCase("traffic")) { // "Current"
                result = new String[]{ref.getTransferString()};
            } else if (function.equalsIgnoreCase("info")) { // "Cumulative"
                
                
                
            }
            
            json.put("header", responseHeader(OK)); // Header Section of the response
            json.put("response", responseSection(result));            
        } catch (JSONException | MalformedObjectNameException | ReflectionException | InstanceNotFoundException ex) {
            Logger.getLogger(StatService.class.getName()).log(Level.SEVERE, null, ex);
            try {
                json.put("header", responseHeader(SEE_OTHER, ex.getLocalizedMessage()));
                json.put("response", responseSection(null));            
            } catch (JSONException ex1) {
                Logger.getLogger(StatService.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        return json;
    }
    
}
