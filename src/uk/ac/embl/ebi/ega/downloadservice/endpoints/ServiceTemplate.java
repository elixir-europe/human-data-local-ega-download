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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import us.monoid.json.JSONArray;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

/**
 *
 * @author asenf
 */
public abstract class ServiceTemplate implements Service {
    
    // Generate JSON Header Section
    protected JSONObject responseHeader(HttpResponseStatus status) throws JSONException {
        return responseHeader(status, "");
    }
    protected JSONObject responseHeader(HttpResponseStatus status, String error) throws JSONException {
        JSONObject head = new JSONObject();
        
        head.put("apiVersion", "v2");
        head.put("code", String.valueOf(status.code()));
        head.put("service", "ds");
        head.put("technicalMessage", "");                   // TODO (future)
        head.put("userMessage", status.reasonPhrase());
        head.put("errorCode", String.valueOf(status.code()));
        head.put("docLink", "http://www.ebi.ac.uk/ega");    // TODO (future)
        head.put("errorStack", error);                     // TODO ??
        
        return head;
    }

    // Generate JSON Response Section
    protected JSONObject responseSection(String[] arr) throws JSONException {
        JSONObject response = new JSONObject();

        response.put("numTotalResults", 1); // -- Result = 1 Array -- (?)
        response.put("resultType", "us.monoid.json.JSONArray");
        
        JSONArray mJSONArray = arr!=null?new JSONArray(Arrays.asList(arr)):new JSONArray();        
        response.put("result", mJSONArray);
        
        return response;
    }

    // Decodes the body of an HTTP POST request, places them in HashMap 'values' (passed in)
    protected int decodeRequestBody(FullHttpRequest request, String formname, Map<String, String> values) throws JSONException, IOException {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(false), request);

        InterfaceHttpData bodyHttpData = decoder.getBodyHttpData(formname);
        JSONObject json = new JSONObject(((Attribute)bodyHttpData).getValue());
        
        int v = 0;
        Set<String> keySet = values.keySet();
        if (!keySet.isEmpty()) {
            Iterator<String> iter = keySet.iterator();
            
            while (iter.hasNext()) {
                String key = iter.next();
                String put = values.put(key, json.get(key).toString());
                if (put!=null && put.length() > 0)
                    v++;
            }
        }
        
        // Done - values passed back in calling argument. Return number of decoded values
        return v;
    }
}
