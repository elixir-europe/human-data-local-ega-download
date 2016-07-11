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
import java.util.ArrayList;
import java.util.Map;
import uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadService;
import us.monoid.json.JSONObject;

/*
 * TODO - Download a metadata package from a fixed FTP location
 */
public class MetadataService extends ServiceTemplate implements Service {

    @Override
    public JSONObject handle(ArrayList<String> id, Map<String, String> parameters, FullHttpRequest request, String ip, EgaSecureDownloadService ref) {
        JSONObject json = new JSONObject(); // Start out with common JSON Object

        String id_ = (id!=null&&id.size()>0)?id.get(0):""; // Dataset ID
        
        
        
        return json;
    }
}
