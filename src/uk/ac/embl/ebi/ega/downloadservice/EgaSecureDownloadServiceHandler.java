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

/*
 * This class provides responses to REST URLs
 * This service will run ONLY inside the EGA Vault and is not available anywhere else
 * For this reason it uses plain http and no user account information
 *
 * URL Prefix for his server is: /ega/download/v2
 *
 * Resources are: 
 *
 *      /Downloads/{downloadticket}  Start downloading a re-encrypted file
 *
 *      /Results/{downloadticket}  Obtain server statistics (size, md5) of the download after completion
 *
 *      /Metadata/{dataset} Obtain metadata packet for selected Dataset <is public in the website anyway>
 *
 *      /Stats/load                          Server Load (total server CPU 0-100)
 *      /Stats/traffic                       Server Current Transfer Statistics
 *      /Stats/info                          Server Cumulative Transfer Statistics (TODO)
 *
 * Response is formatted BINARY
 * 
 */

package uk.ac.embl.ebi.ega.downloadservice;

import com.google.common.io.CountingInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.DATE;
import static io.netty.handler.codec.http.HttpHeaderNames.EXPIRES;
import static io.netty.handler.codec.http.HttpHeaderNames.LAST_MODIFIED;
import io.netty.handler.codec.http.HttpHeaderUtil;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedStream;
import io.netty.util.CharsetUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import static uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadService.restCall;
import uk.ac.embl.ebi.ega.downloadservice.endpoints.Service;
import uk.ac.embl.ebi.ega.downloadservice.utils.MyCacheEntry;
import uk.ac.embl.ebi.ega.downloadservice.utils.MyNewBackgroundInputStream;
import uk.ac.embl.ebi.ega.downloadservice.utils.MyPipelineUtils;
import uk.ac.embl.ebi.ega.downloadservice.utils.RestyTimeOutOption;
import us.monoid.json.JSONArray;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;
import us.monoid.json.XML;
import us.monoid.web.BinaryResource;
import us.monoid.web.JSONResource;
import us.monoid.web.Resty;
import static us.monoid.web.Resty.content;
import static us.monoid.web.Resty.data;
import static us.monoid.web.Resty.delete;
import static us.monoid.web.Resty.form;

/**
 *
 * This is unique/exclusive for each connection - place user interaction caches here
 */
public class EgaSecureDownloadServiceHandler extends SimpleChannelInboundHandler<FullHttpRequest> { // (1)

    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    public static final int HTTP_CACHE_SECONDS = 60;
    
    public static double load_ceiling = 60.0;
    
    // Handle session unique information
    private MessageDigest md;
    private boolean SSL = false, active = true;
    private final HashMap<String, Service> endpointMappings;
    
    private boolean error = false;
    private String error_message = null;
    private boolean inc = false;

    private InputStream is = null, in = null;
    private DigestInputStream dis_ins = null;
    private CountingInputStream c_out = null;

    private int error_cnt;
    private String ticket_ = "";
    private String lockticket = "", lockip = "";

    private boolean ConnectionReset = false;
    
    private boolean metadata = false;
    
    private long tm;
    private boolean msgRec = false;
    
    private final EgaSecureDownloadService ref;
       
    // New Error Codes (TODO: Test)
    private static HttpResponseStatus REQUEST_ERROR = new HttpResponseStatus(580, "Error Getting Request Header");
    
    public EgaSecureDownloadServiceHandler(boolean SSL, HashMap<String, Service> mappings, EgaSecureDownloadService ref) throws NoSuchAlgorithmException {
        super();
        this.md = MessageDigest.getInstance("MD5");
        this.SSL = SSL;
        this.endpointMappings = mappings;
        
        this.ref = ref; // reference to server object - to have access to statistics
    }
    
    // *************************************************************************
    // *************************************************************************
    @Override
    public void messageReceived(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (ctx==null) return; if (request==null) return; // Don't even proceed in these cases!
        error_message = "";
        
        // Step 1: Get IP; may be contained in a Header
        String get = request.headers().get("Accept").toString(); // Response Type
        String ip = MyPipelineUtils.getIP(ctx, request);

        // Step 2: Check Request
        HttpResponseStatus checkURL = MyPipelineUtils.checkURL(request);
        if (checkURL != OK) {
            error_message = "Request Verification Error.";
            sendError(ctx, checkURL, get);
            return;
        }
        
        // Step 3: Active for Binary Connections??
        if (!EgaSecureDownloadService.keepRunning && !get.contains("application/json")) {
            error_message = "Service shutting down.";
            sendError(ctx, SERVICE_UNAVAILABLE, get); // Service is shutting down
            return;
        }
        
        // Step 4: Redirect EBI-internal requests - set 'iternal' flag based on IP (offload internal load to secondary server)
        boolean internal = false;
        if ( ip.startsWith("172.") || ip.startsWith("10.7.") )
            internal = true;

        // Step 5: process the path (1) verify root and service (2) determine function & resource
        String path = MyPipelineUtils.sanitizedUserAction(request);
        ArrayList<String> id = new ArrayList<>();
        String function = MyPipelineUtils.processUserURL(path, id);
        
        // Step 6: Extract any parameters sent with request
        Map<String, String> parameters = MyPipelineUtils.getParameters(path);
                
        // Past "limiters" and "checkers" - increase count of active connections
        EgaSecureDownloadService.incCount(); inc = true;
        
        error_cnt = 0;
        long time_ = System.currentTimeMillis();
        
        ticket_ = (id!=null && id.size()>0)?id.get(0):"";

        // ---------------------------------------------------------------------
        // Map function to endpoint: individual request (downloads follow later)
        // ---------------------------------------------------------------------
        JSONObject json = null;
        if (this.endpointMappings.containsKey(function)) {            
            json = this.endpointMappings.get(function).handle(id, parameters, request, ip, ref); // Get download information based on ticket
            if (json==null) {
                error_message = "Processing function '" + function + "' produced a null result.";
                System.out.println(error_message);
                sendError(ctx, NOT_FOUND, get); // If the URL Function is incorrect...
                return;
            }
        } else {
            error_message = "Function '" + function + "' is not implemented.";
            System.out.println(error_message);
            sendError(ctx, BAD_REQUEST, get); // If the URL Function is incorrect...
            return;
        }
        error_cnt = 5;
        
        // Hack: If just text was requested - end response here!
        if (function.equalsIgnoreCase("/stats") || function.equalsIgnoreCase("/results")) {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);
            StringBuilder buf = new StringBuilder();
            response.headers().set(CONTENT_TYPE, "application/json");
            buf.append(json.toString());
            error_cnt = 6;

            // Step 4: Result has been obtained. Build response and send to requestor
            ByteBuf buffer = Unpooled.copiedBuffer(buf, CharsetUtil.UTF_8);
            response.content().writeBytes(buffer);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);

            // Cleanup, and update servers, if necessary
            buffer.release();
        
            if (inc) {
                EgaSecureDownloadService.decCount();
                inc = false;
            }
            return;
        } // -------------------------------------------------------------------
        
        // ---------------------------------------------------------------------
        // ---------------------------------------------------------------------
        // ---------------------------------------------------------------------       
        error_cnt = 7;

        // If the code progresses here, it is a request for a data stream
        // It could be a file download or a metadata request
        
        // Temporary variables - need 'final' to pass on to completion function
        String resource = null, server = null, ticket = null, id_ = null;
        String ticket_0_ = null, ticket_1_ = null, ticket_2_ = null, ticket_3_ = null, ticket_4_ = null;
        
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        if (function.equalsIgnoreCase("/downloads")) { // Download a ReEncrypted File &&&&&&&&&&&&&&&&&&&&&&&
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            //// Step 2: POST resource request to RES 
            id_ = (id!=null && id.size()>0)?id.get(0):"";
            JSONObject jsonObject = json.getJSONObject("response");
            
            // Retry... TODO
            
            JSONArray jsonArray = jsonObject.getJSONArray("result");
            ticket = id_;
            String resource_ = null, server_ = null;            
            try { // May not get an answer for that ticket!
                resource_ = jsonArray.getString(0); // RES Resource ID
                server_ = jsonArray.getString(1);   // RES URL
                ticket_0_ = jsonArray.getString(2); // User (email)
                ticket_1_ = jsonArray.getString(3); // EGAF File ID
                ticket_2_ = jsonArray.getString(4); // ?? Not used anyway...
                ticket_3_ = jsonArray.getString(5); // ReEncryption Format (AES128)
                ticket_4_ = jsonArray.getString(6); // File Name
            } catch (Throwable th) {
                System.out.println("Error retrieving ticket " + ticket + " from session.");
                EgaSecureDownloadService.logEvent(ip, "-", ("Access Ticket Error: "+th.getLocalizedMessage()), "Ticket Access Error", ticket, "-");
                EgaSecureDownloadService.log("Access Ticket Error: "+th.getLocalizedMessage()+" -- "+ticket);
                error_message = "Error retrieving ticket " + ticket + " from SessionID service. Check ticket before trying again.";
                sendError(ctx, INTERNAL_SERVER_ERROR, get);
                return;
            }
            resource = resource_;
            server = server_;
            error_cnt = 8;

            // Prevent duplicate concurrent downloads
        //    if (isTicketLocked(ticket, ip)) {
        //        System.out.println("Download of " + ticket + " is already active.");
        //        EgaSecureDownloadService.logEvent(ip, "-", "Attempted duplicate concurrent download.", "Ticket Lock Event", ticket, "-");
        //        error_message = "Download of " + ticket + " is already active.";
        //        sendError(ctx, LOCKED, get);
        //        return;
        //    }


            //if (!EgaSecureDownloadService.UDT) lockTicket(ticket, ip); // Change 'ready' to 'active' in downoad ticket table
            this.lockticket = ticket;
            this.lockip = ip;

            // Step 3: GET Resource and deliver the stream (using the same server where the resource was created).

            // Get the stream from RES on a time-out basis. Wait 15s for stream to be established; re-try 4 times before giving up.        
            is = timerBasedInputFromURLRetry(resource, server, 30, 4);
            error_cnt = 9;
            if (is!=null) dis_ins = new DigestInputStream(is, md);
            if (dis_ins!=null) c_out = new CountingInputStream(dis_ins);
            if (c_out!=null) in = new MyNewBackgroundInputStream(c_out); // Buffer/Read-Ahead on the Stream (separate thread)
            //InputStream in = c_out; // Buffer/Read-Ahead on the Stream;
            error_cnt = 10;
            if (in == null) {
                System.out.println("Error getting ReEncrypted Stream for " + ticket);
                EgaSecureDownloadService.log("Error getting ReEncrypted Stream for " + ticket + "  and resource " + resource);
                EgaSecureDownloadService.logEvent(ip, ticket_0_, (ticket_1_ + " " + ticket_4_), "Error Getting Stream", ticket, "*");
                error_message = "Error getting ReEncrypted Stream for " + ticket + " from RES service!";
                System.out.println(error_message);
                sendError(ctx, INTERNAL_SERVER_ERROR, get);
                return;
            }
            error_cnt = 11;
        } else if (function.equalsIgnoreCase("/metadata")) { // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            // Not Implemented
        }
        
        // POST IF Statement -- Declare final Variables for Completion Step
        
        long fileLength = 0; // get from JSON
        String filePath = ""; // get from JSON
        final String fResource = resource;
        final String fServer = server;
        final String fTicket = ticket;
        String[] r = new String[]{ticket_0_, ticket_1_, ticket_2_, ticket_3_, ticket_4_}; // Save a call to SessionID

        // Wait until stream is actually open before logging start
        if (metadata)
            EgaSecureDownloadService.log("Download Started. " + ticket_ + "  " + "Metadata" + "  " + (is!=null));
        else
            EgaSecureDownloadService.log("Download Started. " + ticket + "  " + r[0] + "  " + (is!=null));

        if (r!=null && r.length>=4 && !metadata)
            EgaSecureDownloadService.logEvent(ip, r[0], (r[1] + " " + r[4]), "download started", ticket, "*");
        
        // Step 4: Prepare a response - set content typt to the expected type
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpHeaderUtil.setContentLength(response, fileLength);        
        HttpHeaderUtil.setTransferEncodingChunked(response, true);
        setContentTypeHeaderBinary(response);
        setDateAndCacheHeaders(response, filePath);
        if (HttpHeaderUtil.isKeepAlive(request)) {
            response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }
        error_cnt = 12;

        // Write the initial line and the header. ------------------------------
        ctx.write(response);

        // Write the content. -------------------------------------------------- Writing the actual Data
        final long time = System.currentTimeMillis();
        ChannelFuture sendFileFuture;
        sendFileFuture =
                ctx.write(new HttpChunkedInput(new ChunkedStream(in, 16384)),
                        ctx.newProgressivePromise());       

        sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
                //if (total < 0) { // total unknown
                    //System.err.println(future.channel() + " Transfer progress: " + progress);
                    //System.out.println(c_out.getCount());
                //} else {
                    //System.err.println(future.channel() + " Transfer progress: " + progress + " / " + total);
                    //System.out.println(c_out.getCount());
                //}
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) {
                String protocol = "http/";
                if (EgaSecureDownloadService.UDT)
                    protocol += "udt";
                else
                    protocol += "tcp";
                    
                if (error && !metadata) { // if an exception was thrown, log it and exit.                    
                    System.out.println("Exception thrown " + lockticket + "  " + error_message);
                    EgaSecureDownloadService.putEntry(lockticket, new MyCacheEntry(ip, "Download Failed", 0));
                    EgaSecureDownloadService.log("Download Failed: " + error_message + " for " + lockticket);
                    EgaSecureDownloadService.logDB(ip, r[0], r[1], (error_cnt + " :: " + error_message), "failure", protocol, "AES*");
                    if (inc) {
                        EgaSecureDownloadService.decCount();
                        inc = false;
                    }
                    try { // Close streams in reverse order
                        error_cnt = 140;
                        if (c_out!=null) c_out.close();
                        if (dis_ins!=null) dis_ins.close();
                        if (is!=null) is.close();
                        if (in!=null) in.close();
                        error_cnt = 150;
                    } catch (IOException ex) {
                        error = true;
                        error_message = "Error Closing Streams (Error): " + ex.getLocalizedMessage() + "  " + lockticket;
                        System.out.println(error_message);
                    }
                    
                    //unlockTicket(lockticket, ip);
                    in = null;
                    is = null;
                    c_out = null;
                    dis_ins = null;
                    
                    if (inc) {
                        EgaSecureDownloadService.decCount();
                        inc = false;
                    }
                    
                    return;
                }
                
                error_cnt = 13;
                
                System.err.println(future.channel() + " Transfer complete.");
                long xfertime = System.currentTimeMillis()-time;
                boolean success = false;
                
                try { // Close streams in reverse order
                    error_cnt = 14;
                    if (c_out!=null) c_out.close();
                    if (dis_ins!=null) dis_ins.close();
                    if (is!=null) is.close();
                    if (in!=null) in.close();
                    error_cnt = 15;
                } catch (IOException ex) {
                    error = true;
                    error_message = "Error Closing Streams: " + ex.getLocalizedMessage() + "  " + lockticket;
                    System.out.println(error_message);
                }

                if (c_out.getCount() == 0) {
                    error = true; // Nothing sent is an error condition!
                    error_message = "Stream length 0";
                    System.out.println(error_message);
                }
                
                if (!error && !metadata) { // set by exception handling, if an error occurred
                    error_cnt = 16;
                    long sent = c_out==null?0:c_out.getCount();
                    byte[] digest = md.digest();
                    BigInteger bigInt = new BigInteger(1,digest);
                    String hashtext = bigInt.toString(16);
                    while(hashtext.length() < 32 ){
                      hashtext = "0"+hashtext;
                    }
                    System.out.println("MD5 = " + hashtext);
                    System.out.println(sent + "  " + fileLength);
                    double rate = (sent * 1.0 / 1024.0 / 1024.0) / (xfertime * 1.0 / 1000.0); // MB/s

                    // check with Server Stats - get results by resource
                    //String[] res = getResult(resource, server); // Using same server as the download used
                    String[] res = getResultTimedRetry(fResource, fServer, 6); // Using same server as the download used
                    if (res != null && res.length>0) { // If there was a problem in RES, there may not be a result
                        error_cnt = 17;
                        for (int i=0; i<res.length; i++) {
                            System.out.println("    " + res[i]);
                        }

                        // Check for success/failure, store in cache
                        long res_sent = Long.parseLong(res[1]);
                        if (res[0].equalsIgnoreCase(hashtext) && res_sent == sent) {
                            error_cnt = 18;
                            System.out.println("Download Success - update log");
                            MyCacheEntry mce = new MyCacheEntry(ip, hashtext, sent);
                            EgaSecureDownloadService.putEntry(fTicket, mce);
                            System.out.println("MD5: " + hashtext + " -- Bytes Sent: " + sent);

                            // New: Remove ticket!!
                            System.out.println("removing ticket! " + fTicket + " " + ip);
                            //removeTicket(ticket, ip);
                            removeTicket(fTicket, ip);
                            success = true;

                            // Log in DB (user, fileid, dspeed, dstatus, dprotocol, fileformat)
                            String format = "http/tcp";
                            if (EgaSecureDownloadService.UDT)
                                format = "http/udt";
                            EgaSecureDownloadService.logDB(ip, r[0], r[1], String.valueOf(rate), "success", format, "AES");
                            EgaSecureDownloadService.logEvent(ip, r[0], (r[1] + " " + r[3] + " -- " + res[0]), "download complete", fTicket, "*");
                            EgaSecureDownloadService.log("Download Complete. " + fTicket);
                            error_cnt = 19;

                        } else {
                            error_cnt = 20;
                            String error = "";
                            if (res_sent != sent)
                                error = "Sizes different: " + res_sent + "<>" + sent;
                            else
                                error = "MD5 different: " + res[0] + "<>" + hashtext;
                            
                            System.out.println("Download Failed " + fTicket + error);
                            MyCacheEntry mce = new MyCacheEntry(ip, "Download Failed", sent);
                            EgaSecureDownloadService.putEntry(fTicket, mce);

                            // Log in DB (user, fileid, "", "failed", dprotocol, fileformat)
                            //EgaSecureDownloadService.logDB(ip, r[0], r[1], "dl interrupted", "failure", "http/tcp", "AES");
                            EgaSecureDownloadService.logDB(ip, r[0], r[1], error, "failure", protocol, "AES*");
                            EgaSecureDownloadService.logEvent(ip, r[0], (r[1] + " " + r[3] + " -- " + res[0] + " -- " + error), "download failed", fTicket, "*");
                            EgaSecureDownloadService.log("Download Failed. " + fTicket + error);

                        }
                    } else { // result obtained from res is null
                        String msg = "Obtaining RES result failed";
                        if (ConnectionReset)
                            msg = "Connection Reset by Peer";
                        else if (error)
                            msg = "DS Exception";
                        System.out.println(msg + " " + fTicket);
                        MyCacheEntry mce = new MyCacheEntry(ip, "Download Failed", sent);
                        EgaSecureDownloadService.putEntry(fTicket, mce);

                        // Log in DB (user, fileid, "", "failed", dprotocol, fileformat) user..file..
                        EgaSecureDownloadService.logDB(ip, r[0], r[1], msg, "failure", protocol, "AES*");
                        //EgaSecureDownloadService.logEvent(ip, id_, (r[1] + " " + r[3] + " -- " + res[0]), msg, fTicket, "*");
                        if (r.length>3) EgaSecureDownloadService.logEvent(ip, fTicket, (r[1] + " " + r[3] + " -- " + res[0]), msg, fTicket, "*");
                        EgaSecureDownloadService.log("Download Failed ("+msg+"). " + fTicket);
                    }
                } else if (!metadata) { // If error state has been set, log this error
                    System.out.println("Exception thrown " + fTicket + "  " + error_message);
                    EgaSecureDownloadService.putEntry(fTicket, new MyCacheEntry(ip, "Download Failed", 0));
                    EgaSecureDownloadService.log("Download Failed: " + error_message + " for " + fTicket);
                    EgaSecureDownloadService.logDB(ip, r[0], r[1], error_message, "failure", protocol, "AES*");
                }

                //if (!success) unlockTicket(ticket, ip); // change 'active' to 'ready'
                error_cnt = 21;
                
                // Done!
                in = null;
                is = null;
                c_out = null;
                dis_ins = null;

                if (inc) {
                    EgaSecureDownloadService.decCount();
                    inc = false;
                }
                error_cnt = 22;
                
            }
        });

        // Write the end marker
        ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        // Decide whether to close the connection or not.
        if (!HttpHeaderUtil.isKeepAlive(request)) {
            // Close the connection when the whole content is written out.
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        this.error = true;
        this.error_message = cause.getLocalizedMessage();
        if (this.error_message.equalsIgnoreCase("Connection reset by peer"))
            this.ConnectionReset = true;
        EgaSecureDownloadService.logEvent("*", "*", (error_cnt + " :: " + cause.getLocalizedMessage()), "ds exception: ", ticket_, "*");
        //unlockTicket(this.lockticket, this.lockip);
        if (ctx.channel().isActive()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }        
    }
    
    // JSON Version of error messages
    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        sendError(ctx, status, "application/json");
    }
    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String get) {
        EgaSecureDownloadService.log(status.toString());
        try {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);
            JSONObject json = new JSONObject(); // Start out with common JSON Object
            json.put("header", responseHeader(status)); // Header Section of the response
            json.put("response", "null"); // ??
            
            StringBuilder buf = new StringBuilder();
            if (get.contains("application/json") || get.contains("application/octet-stream")) { // Format list of values as JSON
                response.headers().set(CONTENT_TYPE, "application/json");
                buf.append(json.toString());
            } else if (get.contains("xml")) { // Format list of values as XML
                response.headers().set(CONTENT_TYPE, "application/xml");
                String xml = XML.toString(json);
                buf.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                buf.append("<Result>");
                buf.append(xml);
                buf.append("</Result>");
            }
            
            ByteBuf buffer = Unpooled.copiedBuffer(buf, CharsetUtil.UTF_8);
            response.content().writeBytes(buffer);
            
            // Close the connection as soon as the error message is sent.
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } catch (JSONException ex) {;}
        if (inc) {
            EgaSecureDownloadService.decCount();
            inc = false;
        }
    }

    /**
     * Sets the Date and Cache headers for the HTTP Response
     *
     * @param response
     *            HTTP response
     * @param fileToCache
     *            file to extract content type
     */
    private static void setDateAndCacheHeaders(HttpResponse response, String fileToCache) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        // Date header
        Calendar time = new GregorianCalendar();
        response.headers().set(DATE, dateFormatter.format(time.getTime()));

        // Add cache headers
        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
        response.headers().set(EXPIRES, dateFormatter.format(time.getTime()));
        response.headers().set(CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
        Date x = new Date();
        x.setTime(System.currentTimeMillis()-1000000);
        response.headers().set(
                LAST_MODIFIED, dateFormatter.format(x));
    }

    /**
     * Sets the content type header for the HTTP Response
     *
     * @param response
     *            HTTP response
     * @param file
     *            file to extract content type
     */
    private static void setContentTypeHeaderBinary(HttpResponse response) {
        response.headers().set(CONTENT_TYPE, "application/octet-stream");
    }

    private static InputStream getResource(String resource, String server) {
        InputStream in = null;
        
        Resty r = new Resty();
        try {
            //String server = EgaSecureDownloadService.getServer("res");
            BinaryResource bytes = r.bytes(server+"/downloads/"+resource);
            in = bytes.stream();
        } catch (IOException ex) {;}
        
        return in;
    }
    private static InputStream timerBasedInputFromURLRetry(String resource, String server, int seconds, int retry) {
        InputStream in = null;
        
        URL url;
        try {
            url = new URL(server+"/downloads/"+resource);
            System.out.println("Getting stream resource from RES now: " + url.toString());
            int retryCount = 1;
            while (in==null && retry-->0) {
                in = timerBasedInputFromURL(url, (seconds * retryCount));
                if (in == null) {
                    try {
                        Thread.sleep(472 * retryCount++);
                    } catch (InterruptedException ex) {;}
                }
            }

        } catch (MalformedURLException ex) {;}
        if (in == null) in = getResource(resource, server);
        
        return in;
    }
    private static InputStream timerBasedInputFromURL(final URL url, int seconds) {
        InputStream in = null;
        
        // Timer-based operation, to prevent the system "hanging"
        Future y = null;

        Callable callable = new Callable() {
            @Override
            public InputStream call() throws Exception {
                System.out.println("Establishing URL Connection: " + url);
                HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();//connect
                urlConn.setRequestMethod("GET");
                //urlConn.setRequestProperty("Content-Type", "application/octet-stream");
                urlConn.setRequestProperty("Accept", "application/octet-stream");
                InputStream in_ = urlConn.getInputStream();
                
                if (urlConn.getResponseCode() != 200) {
                    System.out.println(urlConn.getResponseCode() + " : " + urlConn.getResponseMessage());
                    System.out.println("errorStack: ");
                    System.out.println(urlConn.getHeaderField("errorStack"));
                }
                
                return in_;
            }
        };
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<InputStream> res = exec.submit(callable);

        try {
            in = res.get(seconds, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException ex) {
            System.out.println("ex: " + ex.getLocalizedMessage());
            System.out.println("Timout event. Retrying connection.");
        }
        if (!res.isDone()) res.cancel(true);
        exec.shutdown();                
        
        return in;
    }
    
    public static JSONResource timerBasedURLCallForm(final String url, final JSONObject json, final String formname, final int seconds) {
        
        final Resty r = new Resty();

        // Timer-based operation, to prevent the system "hanging"
        JSONResource json1 = null;
        Future y = null;

        Callable callable = new Callable() {
            @Override
            public JSONResource call() throws Exception {
                return r.json(url, form( data(formname, content(json)) ));
            }
        };
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<JSONResource> res = exec.submit(callable);

        try {
            json1 = res.get(seconds, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException ex) {
            Logger.getLogger(EgaSecureDownloadService.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (!res.isDone()) res.cancel(true);
        exec.shutdown();                
        
        
        return json1;
    }
    
    private static String[] getResult(String resource, String server) {
        String[] result = null;
        
        Resty r = new Resty();
        try {
            URL url = new URL(server+"/results/"+resource);
            JSONResource json1 = restCall(r,url.toString(), null, null);

            JSONObject jobj = (JSONObject) json1.get("response");
            JSONArray jsonarr = (JSONArray)jobj.get("result");

            result = new String[jsonarr.length()];
            for (int i=0; i<jsonarr.length(); i++)
                result[i] = jsonarr.getString(i);
        } catch (Exception ex) {
            System.out.println(ex.getLocalizedMessage());
            Logger.getLogger(EgaSecureDownloadServiceHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return result;
    }
    private static String[] getResultTimedRetry(String resource, String server, int retries) {
        String[] result = null;
        
        while ((result == null || result.length==0) && retries-- > 0) {
            result = getResultTimed(resource, server);
            if (result == null || result.length==0) {
                try {
                    Thread.sleep(959);
                } catch (InterruptedException ex) {;}
            }
        }

        return result;
    }
    private static String[] getResultTimed(String resource, String server) {
        String[] result = null;
        
        Resty r_ = new Resty(new RestyTimeOutOption(8000, 5000));
        try {
            URL url = new URL(server+"/results/"+resource);

            JSONResource json1 = r_.json(url.toURI());

            JSONObject jobj = (JSONObject) json1.get("response");
            JSONArray jsonarr = (JSONArray)jobj.get("result");

            JSONObject jobjheader = (JSONObject) json1.get("header");
            if (jobjheader!= null && jobjheader.has("errorStack")) {
                String x = jobjheader.getString("errorStack");
                if (x != null && x.length() > 0)
                    System.out.println("get result errorStack: " + x);
            }
            
            result = new String[jsonarr.length()];
            for (int i=0; i<jsonarr.length(); i++)
                result[i] = jsonarr.getString(i);
        } catch (Exception ex) {
            System.out.println(ex.getLocalizedMessage());
            Logger.getLogger(EgaSecureDownloadServiceHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return result;
    }
    
    private static void removeTicket(String ticket, String ip) {
        Resty r = new Resty(new RestyTimeOutOption(15000, 8000));
        try {
            ///requests/delete/AccessTestRequestFile/" + ticket;
            
            String server = EgaSecureDownloadService.getServer("access");
            String url = server + "/apps/tickets/" + ticket + "?key=" + EgaSecureDownloadService.accessKey; //gw6tUg4g1vs7gg";
            //String url = server+"/requests/"+ticket+"?ip="+ip;
            System.out.println("Removing - URL: " + url); // /requests/ticket/delete/{ticket}
            JSONResource json1 = r.json(url, delete());
            //JSONResource json1 = r.json(server+"/requests/"+ticket+"?ip="+ip, delete());

            System.out.println("****");
System.out.println(json1.toString());
//            JSONObject jobj = (JSONObject) json1.get("response");
//            JSONArray jsonarr = (JSONArray)jobj.get("result");
            JSONArray jsonarr = (JSONArray) json1.get("response");

            String[] result = new String[jsonarr.length()];
            for (int i=0; i<jsonarr.length(); i++) {
                result[i] = jsonarr.getString(i);
                System.out.println(jsonarr.getString(i));
            }
        } catch (Exception ex) {;}
    }
/*    
    private static void lockTicket(String ticket, String ip) {
        Resty r = new Resty();
        try {
            String server = EgaSecureDownloadService.getServer("sessionid");
            JSONResource json1 = r.json(server+"/requests/"+ticket+"/lock?ip="+ip, put(content(new JSONObject())));

            JSONObject jobj = (JSONObject) json1.get("response");
            JSONArray jsonarr = (JSONArray)jobj.get("result");

            String[] result = new String[jsonarr.length()];
            for (int i=0; i<jsonarr.length(); i++) {
                result[i] = jsonarr.getString(i);
                System.out.println(jsonarr.getString(i));
            }
        } catch (Exception ex) {
            Logger.getLogger(EgaSecureDownloadServiceHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void unlockTicket(String ticket, String ip) {
        Resty r = new Resty();
        try {
            String server = EgaSecureDownloadService.getServer("sessionid");
            JSONResource json1 = r.json(server+"/requests/"+ticket+"/unlock?ip="+ip, put(content(new JSONObject())));

            JSONObject jobj = (JSONObject) json1.get("response");
            JSONArray jsonarr = (JSONArray)jobj.get("result");

            String[] result = new String[jsonarr.length()];
            for (int i=0; i<jsonarr.length(); i++) {
                result[i] = jsonarr.getString(i);
                System.out.println(jsonarr.getString(i));
            }
        } catch (Exception ex) {
            Logger.getLogger(EgaSecureDownloadServiceHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static boolean isTicketLocked(String ticket, String ip) {
        boolean res = false;
        Resty r = new Resty();
        try {
            String server = EgaSecureDownloadService.getServer("sessionid");
            JSONResource json1 = r.json(server+"/requests/"+ticket+"/locked?ip="+ip, put(content(new JSONObject())));

            JSONObject jobj = (JSONObject) json1.get("response");
            JSONArray jsonarr = (JSONArray)jobj.get("result");

            String[] result = new String[jsonarr.length()];
            for (int i=0; i<jsonarr.length(); i++) {
                if (jsonarr.getString(i).equalsIgnoreCase("active"))
                    res = true;
            }
            System.out.println("Ticket is " + (res?"":"not ") + "locked");
        } catch (Exception ex) {
            Logger.getLogger(EgaSecureDownloadServiceHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return res;
    }
*/    
    // Generate JSON Header Section
    private JSONObject responseHeader(HttpResponseStatus status) throws JSONException {
        return responseHeader(status, error_message);
    }
    private JSONObject responseHeader(HttpResponseStatus status, String error) throws JSONException {
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
    private JSONObject responseSection(String[] arr) throws JSONException {
        JSONObject response = new JSONObject();

        response.put("numTotalResults", 1); // -- Result = 1 Array -- (?)
        response.put("resultType", "us.monoid.json.JSONArray");
        
        JSONArray mJSONArray = new JSONArray(Arrays.asList(arr));        
        response.put("result", mJSONArray);
        
        return response;
    }
    
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (this.msgRec && ctx!=null)
            ctx.fireChannelReadComplete();
    }
}
