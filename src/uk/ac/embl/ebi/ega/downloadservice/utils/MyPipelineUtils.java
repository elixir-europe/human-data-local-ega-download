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
package uk.ac.embl.ebi.ega.downloadservice.utils;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import static io.netty.handler.codec.http.HttpMethod.GET;
import io.netty.handler.codec.http.HttpResponseStatus;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.owasp.html.PolicyFactory;
import org.owasp.html.Sanitizers;
import org.unbescape.html.HtmlEscape;
import uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadService;

/**
 *
 * @author asenf
 * 
 * A collection of functions helpful when pre-processing an HTTP request URL
 */
public class MyPipelineUtils {

    // -------------------------------------------------------------------------
    // Get IP address from which an HTTP Request came --------------------------
    public static String getIP(ChannelHandlerContext ctx, FullHttpRequest request) {
        String ip_ = null;
        try { // Read supplied information. Stop processing on failure
            ip_ = ctx.channel().remoteAddress().toString(); // Remote IP address
            if (ip_.startsWith("/")) ip_ = ip_.substring(1);
            if (ip_.contains(":")) ip_ = ip_.substring(0, ip_.lastIndexOf(":"));

            // Code to deal with potential Proxys (not necessary at EBI at the moment)
            if (EgaSecureDownloadService.getOrganization()!=null && EgaSecureDownloadService.getOrganization().equalsIgnoreCase("CRG-EGA")) {
                String headerIP = request.headers().get("X-Forwarded-For").toString();
                if (headerIP!=null && headerIP.length()>0)
                    ip_ = headerIP;
            }
        } catch (Throwable th) {
            EgaSecureDownloadService.log("580 error: " + th.getMessage());
            String error_message = "Error accessing request header (and/or request IP). " + th.toString();
            return null;
        }
        
        return ip_;
    }
    
    // -------------------------------------------------------------------------
    // Sanitize data received from an HTTP request
    public static String sanitize(FullHttpRequest request) {
        PolicyFactory policy = Sanitizers.FORMATTING.and(Sanitizers.LINKS);
        String safeUri = policy.sanitize(request.uri()); // take directly as provided by client, and sanitize it
        return HtmlEscape.unescapeHtml(safeUri);
    }
    public static String sanitizedUserAction(FullHttpRequest request) {
        PolicyFactory policy = Sanitizers.FORMATTING.and(Sanitizers.LINKS);
        String safeUri = policy.sanitize(request.uri()); // take directly as provided by client, and sanitize it
        String unescapedSafeUri = HtmlEscape.unescapeHtml(safeUri);
        
        URL user_action = null;
        try {
            user_action = new URL("http://" + unescapedSafeUri);
        } catch (MalformedURLException ex) {;}
        String path = user_action.getPath();
        
        return path;
    }

    // -------------------------------------------------------------------------
    // Process URL, get Information
    //      Returns function as return value, populates id array
    public static String processUserURL (String path, ArrayList<String> id) {
        String function = "";
        
        try {
            StringTokenizer token = new StringTokenizer(path, "/");
            String t = token.nextToken();
            if (!t.equalsIgnoreCase("ega")) throw new Exception("URL Incorrect");
            t = token.nextToken();
            if (!t.equalsIgnoreCase("rest")) throw new Exception("URL Incorrect");
            t = token.nextToken();
            if (!t.equalsIgnoreCase("ds") && !t.equalsIgnoreCase("download")) throw new Exception("URL Incorrect");
            t = token.nextToken();
            if (!t.equalsIgnoreCase("v2")) throw new Exception("URL Incorrect");
            function = "/" + token.nextToken().toLowerCase();   // /downloads
            while (token.hasMoreTokens()) {
                id.add(token.nextToken());
            }
        } catch (Throwable t) {;}

        return function;
    }
    
    // -------------------------------------------------------------------------
    // Get URL parameters
    public static Map<String, String> getParameters(String path) {
        Map<String, String> parameters = new LinkedHashMap<>();

        if (path.contains("?")) {
            String path_ = path.substring(path.indexOf("?") + 1);
            String[] pairs = path_.split("&");
            if (pairs!=null) for (String pair : pairs) {
                int idx = pair.indexOf("=");
                try {
                    parameters.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
                } catch (UnsupportedEncodingException ex) {;}
            }
        }
        return parameters;
    }

    // -------------------------------------------------------------------------
    // Basic URL checks
    public static HttpResponseStatus checkURL(FullHttpRequest request) {
        HttpResponseStatus status = OK;

        if (!request.decoderResult().isSuccess()) {
            status = BAD_REQUEST;
        }        
        if (request.method() != GET) {
            status = METHOD_NOT_ALLOWED;
        }
        
        return status;
    }
}
