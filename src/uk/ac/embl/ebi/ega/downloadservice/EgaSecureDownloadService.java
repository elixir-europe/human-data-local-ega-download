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

package uk.ac.embl.ebi.ega.downloadservice;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultExecutorServiceFactory;
import io.netty.util.concurrent.ExecutorServiceFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.control.CompositeCacheManager;
import org.ini4j.Ini;
import org.ini4j.Profile;
import uk.ac.embl.ebi.ega.downloadservice.endpoints.DownloadService;
import uk.ac.embl.ebi.ega.downloadservice.endpoints.MetadataService;
import uk.ac.embl.ebi.ega.downloadservice.endpoints.ResultService;
import uk.ac.embl.ebi.ega.downloadservice.endpoints.Service;
import uk.ac.embl.ebi.ega.downloadservice.endpoints.StatService;
import uk.ac.embl.ebi.ega.downloadservice.utils.Dailylog;
import uk.ac.embl.ebi.ega.downloadservice.utils.EgaFile;
import uk.ac.embl.ebi.ega.downloadservice.utils.EgaTicket;
import uk.ac.embl.ebi.ega.downloadservice.utils.MyCacheEntry;
import uk.ac.embl.ebi.ega.downloadservice.utils.MyTimerTask;
import uk.ac.embl.ebi.ega.downloadservice.utils.RestyTimeOutOption;
import uk.ac.embl.ebi.ega.downloadservice.utils.SSLUtilities;
import us.monoid.json.JSONArray;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;
import us.monoid.web.BinaryResource;
import us.monoid.web.JSONResource;
import us.monoid.web.Resty;
import static us.monoid.web.Resty.content;
import static us.monoid.web.Resty.data;
import static us.monoid.web.Resty.form;

public class EgaSecureDownloadService {

    private static boolean SSL = false;     // SSL Mode Active?
    private static int port = 9227;         // Port this service listens
    private static int cores = 0;           // CPU cores - used to detemine size of thread pools
    private static boolean testMode = false; // Self-Test?
    public static boolean UDT = false;      // Download in UDT Mode?
    private static String path = "";        // Path to INI file
    public static String accessKey = "";    // Key used to verify requests to Access
    private static String filename = "location.ini"; // Name of INI file

    private static Dailylog dailylog;       // Disk-logging object
    private static boolean log = true ;     // To log or not to log
    
    private static String organization; // Where does the service run? Used for localizations
    private static String cacheLocations = "10.50.8.17:8109,10.50.8.16:8109,10.50.8.18:8109"; // Default
    private static String configLocation = "http://localhost:9228/ega/rest/configlog/v2";
    
    // Testing only
    private static String testticket;
    
    // Runtime Cache - holds download resources (i.e. previously requested files)
    //private static Cache<String, MyCacheEntry> theCache;
    private static final String cacheRegionName = "default";
    private static JCS lateralCache;
    private static Timer theTimer;
    
    // Shutdown process: Wait until current operations complete
    static volatile boolean keepRunning = true;
    
    // Shutdown hook
    static volatile int responseCount;
    
    // Executors and traffic counter
    private final DefaultEventExecutorGroup l, s;
    private final ScheduledExecutorService executor;
    private final GlobalTrafficShapingHandler globalTrafficShapingHandler;

    // LOCAL (test) Mode
    public static HashMap<String,String> localPaths;
    
    /*
     * Constructor
     * Setus up shared/lateral Cache - so that download results are available on all load-balanced services
     * Initializes a timer task (for maintenance) - probably can be deleted
     * Sets up thread pools and traffic counter
     */
    public EgaSecureDownloadService(int port, int cores) {
        EgaSecureDownloadService.port = port;
        
        try {
            CompositeCacheManager ccm = CompositeCacheManager.getUnconfiguredInstance();
            Properties props = new Properties();

            props.put("jcs.default","LTCP"); // DC
            props.put("jcs.default.cacheattributes","org.apache.jcs.engine.CompositeCacheAttributes");
            props.put("jcs.default.cacheattributes.MaxObjects","200001");
            props.put("jcs.default.cacheattributes.MemoryCacheName","org.apache.jcs.engine.memory.lru.LRUMemoryCache");
            props.put("jcs.default.cacheattributes.UseMemoryShrinker","true");
            props.put("jcs.default.cacheattributes.MaxMemoryIdleTimeSeconds","3600");
            props.put("jcs.default.cacheattributes.ShrinkerIntervalSeconds","60");
            props.put("jcs.default.elementattributes","org.apache.jcs.engine.ElementAttributes");
            props.put("jcs.default.elementattributes.IsEternal","false");
            
            if (EgaSecureDownloadService.cacheLocations!=null && EgaSecureDownloadService.cacheLocations.length()>0) {
                props.put("jcs.auxiliary.LTCP","org.apache.jcs.auxiliary.lateral.socket.tcp.LateralTCPCacheFactory");
                props.put("jcs.auxiliary.LTCP.attributes","org.apache.jcs.auxiliary.lateral.socket.tcp.TCPLateralCacheAttributes");
                props.put("jcs.auxiliary.LTCP.attributes.TcpServers",EgaSecureDownloadService.cacheLocations);
                props.put("jcs.auxiliary.LTCP.attributes.UdpDiscoveryEnabled","false");
                props.put("jcs.auxiliary.LTCP.attributes.TcpListenerPort","8109");
                //props.put("jcs.auxiliary.LTCP.attributes.UdpDiscoveryAddr","224.0.0.1");
                //props.put("jcs.auxiliary.LTCP.attributes.UdpDiscoveryPort","6780");
                //props.put("jcs.auxiliary.LTCP.attributes.UdpDiscoveryEnabled","true");        
            }
            props.put("jcs.auxiliary.RC.cacheeventlogger","org.apache.commons.jcs.engine.logging.CacheEventLoggerDebugLogger");
            
            ccm.configure(props);            
            
            EgaSecureDownloadService.lateralCache = JCS.getInstance(cacheRegionName);
        } catch (CacheException ex) {
            System.out.println("Can't create Lateral Cache!");
            System.exit(99);
        }
        
        TimerTask timerTask = new MyTimerTask();
        theTimer = new Timer(true);
        theTimer.scheduleAtFixedRate(timerTask, 900000, 900000);
        
        // TEMP: Until Load Balancer/SSL Endpoint is in Use (deal with self-signed certificate)
        SSLUtilities.trustAllHostnames();
        SSLUtilities.trustAllHttpsCertificates();

        // Executors
        this.l = new DefaultEventExecutorGroup(cores * 4);
        this.s = new DefaultEventExecutorGroup(cores * 4);
        
        // Traffic Shaping Handler already created
        this.executor = Executors.newScheduledThreadPool(cores);
        this.globalTrafficShapingHandler = new GlobalTrafficShapingHandler(executor, cores);
        this.globalTrafficShapingHandler.trafficCounter().configure(10000); // ??
    }
    
    /*
     * Run the service - set up pipeline, listen
     */
    public void run(HashMap<String, Service> mappings) throws Exception {
        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            SelfSignedCertificate ssc = new SelfSignedCertificate(); // DEVELOPMENT
            sslCtx = SslContext.newServerContext(SslProvider.JDK, ssc.certificate(), ssc.privateKey());
        } else {
            sslCtx = null;
        }
        
        if (!UDT)
            runTCP(mappings, sslCtx);
        else
            runUDT(mappings, sslCtx);        
    }

    /*
     * Set up a pipeline for TCP based traffic
     */
    private void runTCP(HashMap<String, Service> mappings, SslContext sslCtx) throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup(EgaSecureDownloadService.cores);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             //.handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new EgaSecureDownloadServiceInitializer(sslCtx, 
                                                                   mappings, 
                                                                   this.l, 
                                                                   this.s, 
                                                                   this.globalTrafficShapingHandler, 
                                                                   this));

            Channel ch = b.bind(port).sync().channel();

            System.err.println("Open your web browser and navigate to " +
                    (SSL? "https" : "http") + "://127.0.0.1:" + port + '/');

            if (testMode)
                testMe();
        
            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
    
    /*
     * Set up a pipeline for UDT based traffic
     */
    private void runUDT(HashMap<String, Service> mappings, SslContext sslCtx) throws Exception {
        ExecutorServiceFactory acceptFactory = new DefaultExecutorServiceFactory("accept");
        ExecutorServiceFactory connectFactory = new DefaultExecutorServiceFactory("connect");
        NioEventLoopGroup acceptGroup = new NioEventLoopGroup(1, acceptFactory, NioUdtProvider.BYTE_PROVIDER);
        NioEventLoopGroup connectGroup = new NioEventLoopGroup(EgaSecureDownloadService.cores, connectFactory, NioUdtProvider.BYTE_PROVIDER);        
        
        try {
            final ServerBootstrap b = new ServerBootstrap();
            b.group(acceptGroup, connectGroup)
                    .channelFactory(NioUdtProvider.BYTE_ACCEPTOR)
                    .option(ChannelOption.SO_BACKLOG, 10)
                    .childHandler(new EgaSecureDownloadServiceInitializerUDT(sslCtx, 
                                                                             mappings, 
                                                                             this.l, 
                                                                             this.s, 
                                                                             this.globalTrafficShapingHandler, 
                                                                             this));

            Channel ch = b.bind(port).sync().channel();

            System.err.println("Open your web browser and navigate to (udt)" +
                    (SSL? "https" : "http") + "://127.0.0.1:" + port + '/');

            if (testMode)
                testMe();
        
            ch.closeFuture().sync();
        } finally {
            acceptGroup.shutdownGracefully();
            connectGroup.shutdownGracefully();
        }
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    /*
     * Request URL of a server based on the requested TYPE
     */
    public static String getServer(String type) {
        String result = null;
        
        String svc = type.toLowerCase().trim();
        if (!svc.equals("access") && !svc.equals("res") && !svc.equals("configlog"))
            return result;
        
        Resty r = new Resty();
        try {
            // Get Info from Config (Which must run on Localhost, for now)
            String sql = EgaSecureDownloadService.configLocation + "/services/" + svc;
            JSONResource json = restCall(r,sql, null, null);
            JSONObject jobj = (JSONObject) json.get("response");
            JSONArray jsonarr1 = (JSONArray)jobj.get("result");
            if (jsonarr1.length() >=1 ) {
                JSONObject jobj2 = (JSONObject)jsonarr1.get(0); // There should ever only be 1

                result = jobj2.getString("protocol") + ((jobj2.getString("protocol").endsWith(":"))?"//":"://") + 
                            jobj2.getString("server") + ":" + 
                            jobj2.getString("port") + "/" + 
                            jobj2.getString("baseUrl") +
                            jobj2.getString("name") + "/" + 
                            jobj2.getString("version");
            }
        } catch (Throwable t) {System.out.println(t.toString());}
        
        return result;
    }

    // GET - Traffic Information
    public String getTransferString() {
        return this.globalTrafficShapingHandler.trafficCounter().toString();
    }
    public JSONObject getTransfer() {
        JSONObject traffic = new JSONObject();
        
        try {
            traffic.put("checkInterval", this.globalTrafficShapingHandler.trafficCounter().checkInterval());

            // Add more...
            
        } catch (JSONException ex) {;}
        
        return traffic;
    }
    
    /**
     * @param args the command line arguments
     * 
     * Parameters: port number (default 9127)
     *      -l config file path
     *      -f config file filename
     *      -p port : server port (default 9127)
     *      -t ticket : testMe
     */
    public static void main(String[] args) {
        String p = "9227"; int pi = 9227;
        
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
               System.out.println("Shutdown Initiated");
                
                keepRunning = false;
                long start = System.currentTimeMillis();
                try {
//                    mainThread.join();                    
                    long delta = System.currentTimeMillis();
                    while (EgaSecureDownloadService.responseCount > 0  && 
                            (delta-start < 86400000) ) {
                        Thread.sleep(1000);
                        delta = System.currentTimeMillis();
                    }
                } catch (InterruptedException ex) {;}

                System.out.println("Shutdown!!");
             }
        });
        
        int cores = Runtime.getRuntime().availableProcessors();

        Options options = new Options();

        options.addOption("l", true, "config file path");        
        options.addOption("f", true, "config file filename");        
        options.addOption("p", true, "port");
        options.addOption("cl", true, "configUrl");        
        options.addOption("u", false, "UDT Mode");
        options.addOption("t", true, "testMe");
        options.addOption("n", false, "noLog");
        
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse( options, args);
            
            if (cmd.hasOption("l"))
                path = cmd.getOptionValue("l");
            if (cmd.hasOption("f"))
                filename = cmd.getOptionValue("f");
            if (cmd.hasOption("p"))
                p = cmd.getOptionValue("p");
            if (cmd.hasOption("cl"))
                EgaSecureDownloadService.configLocation = cmd.getOptionValue("cl");
            if (cmd.hasOption("u"))
                EgaSecureDownloadService.UDT = true;
            if (cmd.hasOption("t")) {
                EgaSecureDownloadService.testMode = true;
                EgaSecureDownloadService.testticket = cmd.getOptionValue("t");
            }
            if (cmd.hasOption("n"))
                log = false;
            if (path!=null && path.length()>0 && !path.endsWith("/"))
                path = path + "/";
            
            pi = Integer.parseInt(p);
        } catch (ParseException ex) {
            System.out.println("Unrecognized Parameter. Use '-l'  '-f' '-p'  '-cl'  -u' '-t'.");
            //Logger.getLogger(EgaSecureDownloadService.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Read Ini File, Get Location-Unique Setup information
        Ini ini = null;
        try {
            ini = new Ini(new File(path + filename));
        } catch (IOException ex) {;}
        
        // Read initialization file (location/org-specific values)
        if (ini != null) {
            Profile.Section section = ini.get("location");
            
            if (section.containsKey("org"))
                organization = section.get("org");
            if (section.containsKey("cache"))
                cacheLocations = section.get("cache");
            if (section.containsKey("ds"))
                cores = Integer.parseInt(section.get("ds"));
            if (section.containsKey("key"))
                accessKey = section.get("key");
            
            if (ini.containsKey("local")) {
                EgaSecureDownloadService.localPaths = new HashMap<>();
                Profile.Section localsection = ini.get("local");
                
                Set<String> keySet = localsection.keySet();
                if (keySet != null) {
                    Iterator<String> iter = keySet.iterator();
                    while (iter.hasNext()) {
                        String key = iter.next();
                        String val = localsection.get(key);
                        
                        EgaSecureDownloadService.localPaths.put(key, val);
                    }
                }
            }
            
        } else {
            System.out.println("Can't read INI file: " + path + filename);
            return;
        }
        
        // Get location of Access & RES. Refuse to start if not available
        Resty r = new Resty();
        String sql = "";        
        try {
            // Get SessionID
            sql = EgaSecureDownloadService.configLocation + "/services/access"; // ...for now!
            JSONResource json = restCall(r, sql, null, null);
            JSONObject jobj = (JSONObject) json.get("response");
            JSONArray jsonarr = (JSONArray)jobj.get("result");
            
            if (jsonarr.length() == 0) {
                System.out.println("Config Reports no Access Services available. Exiting...");
                return;
            }
            
            // Get RES
            sql = EgaSecureDownloadService.configLocation + "/services/res";
            json = restCall(r, sql, null, null);
            jobj = (JSONObject) json.get("response");
            jsonarr = (JSONArray)jobj.get("result");
            
            if (jsonarr.length() == 0) {
                System.out.println("Config Reports no RES Services available. Exiting...");
                return;
            }
        } catch (Exception ex) {
            System.out.println("Config not found. Exiting...");
            System.out.println(ex.toString());
            return;
        }
        
        // Add Service Endpoints
        DownloadService downloadService = new DownloadService();
        MetadataService metadataService = new MetadataService();
        ResultService resultService = new ResultService();
        StatService statService = new StatService();
        
        HashMap<String, Service> mappings = new HashMap<>();
        mappings.put("/downloads", downloadService);
        mappings.put("/metadata", metadataService);
        mappings.put("/results", resultService);
        mappings.put("/stats", statService);
        
        // Set up Log File
        EgaSecureDownloadService.dailylog = null;
        if (log) EgaSecureDownloadService.dailylog = new Dailylog("download");

        // Start and run the server
        try {
            new EgaSecureDownloadService(pi, cores).run(mappings);
        } catch (Exception ex) {
            Logger.getLogger(EgaSecureDownloadService.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
        
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    /*
     * Write a log message to a log file (new file every 24h)
     */
    public static void log(String text) {
        if (log && !text.equalsIgnoreCase("303 See Other")) 
            EgaSecureDownloadService.dailylog.log(text);
    }

    /*
     * Write an event to the logging database, by calling the appropriate ConfigLog service
     */
    public static void logEvent(String ip, String user, String text, String type, String ticket, String session) {
        String server = configLocation;

        Resty r = new Resty();
        try {
            JSONObject json2 = new JSONObject();
            json2.put("user", user);
            json2.put("ip", ip);
            json2.put("event", text);
            json2.put("ticket", ticket);
            json2.put("session", session);
            json2.put("type", type);
            JSONResource json = restCall(r,(server + "/events/event"), json2, "eventrequest");
            JSONObject jobj = (JSONObject) json.get("response");
        } catch (Exception ex) {
            System.out.println(ex.getLocalizedMessage());
            System.out.println("LoS Connection Problems. Exiting...");
            return;
        }
    }
    
    // -------------------------------------------------------------------------

    /*
     * Get a Cache entry - will contain information about a file transfer
     */
    public static MyCacheEntry getEntry(String key) {
        return (MyCacheEntry) EgaSecureDownloadService.lateralCache.get(key);
    }

    /*
     * Add an entry to the lateral cache
     */
    public static void putEntry(String key, MyCacheEntry mce) {
        try {
            EgaSecureDownloadService.lateralCache.put(key, mce);
            System.out.println("Success? " + (EgaSecureDownloadService.lateralCache.get(key) != null));
        } catch (CacheException ex) {System.out.println("Cache Error: " + ex.getMessage());}
    }

    /*
     * Log a entry in the logging database, via call to ConfigLog
     */
    public static void logDB(String ip, String user, String fileid, String dspeed, String dstatus, String dprotocol, String fileformat) {
        if (log) {
            String server = configLocation;

            Resty r = new Resty(new RestyTimeOutOption(8000, 5000)); // read, connect
            try {
                // Step 1: Exiting entry with that ID? - Delete
                
                // TODO...?
                
                // Step 2: Write current entry
                JSONObject json1 = new JSONObject();
                json1.put("user", user);
                json1.put("ip", ip);
                json1.put("server", "ds");
                json1.put("fileid", fileid);
                json1.put("dspeed", dspeed);
                json1.put("dstatus", dstatus);
                json1.put("dprotocol", dprotocol);
                json1.put("fileformat", fileformat);
                JSONResource json = restCall(r, (server + "/entries/entry"), json1,"logrequest");
                JSONObject jobj = (JSONObject) json.get("response");
            } catch (Exception ex) {
                System.out.println(ex.getLocalizedMessage());
                System.out.println("LoS Connection Problems. Exiting...");
                return;
            }
        }
    }

    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    /*
     * Get details about a ticket from Access service
     */
    public static EgaTicket[] getTicket(String ticket, String ip) {
        EgaTicket[] result = null;

        Resty r = new Resty(new RestyTimeOutOption(8000, 5000)); // read, connect
            
        try {
            String server = EgaSecureDownloadService.getServer("access");
            String query = server + "/apps/tickets/" + ticket + "?key=" + accessKey; // Verify to service
System.out.println("Query: " + query);
            
            JSONResource json = restCall(r, query, null, null);
JSONObject jh = (JSONObject) json.get("header");
log("Ticket Query: " + query + "  " + jh.getString("code"));
            int code = jh.getInt("code");
            if (code == 200) {
                JSONObject jobj = (JSONObject) json.get("response");
                JSONArray jsnarr_ = (JSONArray)jobj.get("result");

                result = new EgaTicket[jsnarr_.length()]; // Should be 0 or 1
                for (int i=0; i<jsnarr_.length(); i++) {
                    JSONObject oneTicket = (JSONObject) jsnarr_.get(i);

                    String ticket_ = oneTicket.getString("ticket");
                    String label = oneTicket.getString("label");
                    String fileID = oneTicket.getString("fileID");
                    String fileType = oneTicket.getString("fileType");
                    String fileSize = oneTicket.getString("fileSize");
                    String fileName = oneTicket.getString("fileName");
                    String encryptionKey = oneTicket.getString("encryptionKey");
                    String transferType = oneTicket.getString("transferType");
                    String transferTarget = oneTicket.getString("transferTarget");
                    String user = oneTicket.getString("user");

                    result[i] = new EgaTicket(ticket_, label, fileID, fileType, fileSize, fileName, encryptionKey, transferType, transferTarget, user);
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(EgaSecureDownloadService.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return result; // info for 1 ticket: user, file, encryption key, rel path
    }
    
    public static EgaFile getFile(String fileid) {
        EgaFile result = null;

        Resty r = new Resty(new RestyTimeOutOption(8000, 5000)); // read, connect
            
        try {
            String server = EgaSecureDownloadService.getServer("access");
            String query = server + "/apps/files/" + fileid + "?key=" + accessKey; // Verify to service
System.out.println("Query: " + query);
            
            JSONResource json = restCall(r, query, null, null);
JSONObject jh = (JSONObject) json.get("header");
log("File Query: " + query + "  " + jh.getString("code"));
            int code = jh.getInt("code");
            if (code == 200) {
                JSONObject jobj = (JSONObject) json.get("response");
                JSONArray jsnarr_ = (JSONArray)jobj.get("result");

                for (int i=0; i<jsnarr_.length(); i++) {
                    JSONObject oneFile = (JSONObject) jsnarr_.get(i);

                    String fileID = oneFile.getString("fileID");
                    String fileName = oneFile.getString("fileName");
                    String fileIndex = oneFile.getString("fileIndex");
                    String fileDataset = oneFile.getString("fileDataset");
                    String fileSize = oneFile.getString("fileSize");
                    String fileMD5 = oneFile.getString("fileMD5");
                    String fileStatus = oneFile.getString("fileStatus");

                    result = new EgaFile(fileID, fileName, fileIndex, fileDataset, Long.parseLong(fileSize), fileMD5, fileStatus);
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(EgaSecureDownloadService.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return result; // info for 1 ticket: user, file, encryption key, rel path
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    public static String getOrganization() {
        return EgaSecureDownloadService.organization;
    }
    
    public static void incCount() { // Test: keep track of number of current requests in process
        EgaSecureDownloadService.responseCount++;
    }
    
    public static void decCount() {
        if (EgaSecureDownloadService.responseCount>0) EgaSecureDownloadService.responseCount--;
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    public static double getSystemCpuLoad() throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {

        MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
        ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
        AttributeList list = mbs.getAttributes(name, new String[]{ "SystemCpuLoad" });

        if (list.isEmpty())     return Double.NaN;

        Attribute att = (Attribute)list.get(0);
        Double value  = (Double)att.getValue();

        if (value == -1.0)      return Double.NaN;  // usually takes a couple of seconds before we get real values

        return ((int)(value * 1000) / 10.0);        // returns a percentage value with 1 decimal point precision
    }
    
    /*
     * Helper function to make a REST call
     */
    public static JSONResource restCall(Resty r, String url, JSONObject jsn, String formname) {
        JSONResource json = null;
        if (url==null || url.length()==0)
            return json;
        
        if (r==null)
            r = new Resty(new RestyTimeOutOption(10000, 10000));
        
        boolean errorCondition = true;
        int countdown = 10;
 
        // Retry REST calls up to 10 times before giving up
        while (errorCondition && countdown-- > 0) {
            try {
                // Call - depending on what data is supplied
                if (jsn==null) {
                    json = r.json(url);
                } else if (jsn!=null && formname == null) {
                    // Submit without form name - not needed at the moment
                } else if (jsn!=null && formname!=null && formname.length()>0) {
                    json = r.json(url, form( data(formname, content(jsn)) )); // Uses Timout Class
                }
                errorCondition = false;
            } catch (IOException ex) {;}
            
            // test that there is a result
            if (json == null) {
                errorCondition = true;
            } else {
                try {JSONObject jobj = (JSONObject) json.get("response");} catch (Throwable th) {
                    errorCondition = true;
                }
            }
            
            // In case of error, wait a bit, up to 4 seconds
            if (errorCondition) {
                Random x = new Random(4000);
                try {
                    long y = Math.abs(x.nextInt(4000));
                    Thread.sleep((y>4000?4000:y));
                } catch (InterruptedException ex) {;}
            }
        }
        
        // retrn the result object; if all else fails, return null
        return json;
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // Self-Test of functionality provided in this server
    private void testMe() throws Exception {
        
        // Not currently updated for Local EGA
        
        // Wait until server has started up
        Thread.sleep(2000);
        System.out.println("Selftest");
        
        Resty r = new Resty();
        if (EgaSecureDownloadService.testticket.equalsIgnoreCase("stream")) {
            System.out.println("Testing Download - 1 ----------------");
            String tt = "237fey60-c49a-4a6f-w49b-3eec40bcd778";
            String query = "http://localhost:" + EgaSecureDownloadService.port + 
                    "/ega/rest/download/v2/downloads/" + tt + "?ip=127.0.0.1";        
            long time = System.currentTimeMillis();
            BinaryResource bytes = r.bytes(query); // Download -- byte stream
            InputStream in = bytes.stream();
            long cnt = 0, tot = 0;
            byte[] buffer = new byte[64000];
            cnt = in.read(buffer);
            while (cnt > 0) {
                tot += cnt;
                cnt = in.read(buffer);
            }
            time = System.currentTimeMillis() - time;
            System.out.println("tot="+tot);
            double rate = (tot * 1.0 / 1024.0 / 1024.0) / (time * 1.0 / 1000.0);
            System.out.println("tot="+tot+" /  Rate Test Stream (MB/s): "+rate);
        }
        else // Next section needs an existing valid ticket to test download
        {
            System.out.println("Testing Download - 2 ----------------");
            String downloadticket = EgaSecureDownloadService.testticket;
            String query = "http://localhost:" + EgaSecureDownloadService.port + 
                    "/ega/rest/download/v2/downloads/" + downloadticket;
            long time = System.currentTimeMillis();
            BinaryResource bytes = r.bytes(query); // Download -- byte stream
            InputStream in = bytes.stream();
            long cnt = 0, tot = 0;
            byte[] buffer = new byte[64000];
            cnt = in.read(buffer);
            while (cnt > 0) {
                tot += cnt;
                cnt = in.read(buffer);
            }
            time = System.currentTimeMillis() - time;
            System.out.println("tot="+tot);
            double rate = (tot * 1.0 / 1024.0 / 1024.0) / (time * 1.0 / 1000.0);
            System.out.println("tot="+tot+" /  Rate Actual File Stream (MB/s): "+rate);
        }
        
        // Testing Stats -------------------------------------------------------
        System.out.println("Testing Stats ----------------");
        String query = "http://localhost:" + EgaSecureDownloadService.port + 
                "/ega/rest/download/v2/stats/load";
        JSONResource json = restCall(r, query, null, null);
        JSONObject jobj = (JSONObject) json.get("response");
        JSONArray jsonarr = (JSONArray)jobj.get("result");
        for (int i=0; i<jsonarr.length(); i++) {
            System.out.println("  " + i + " : " + jsonarr.getString(i));
        }
        
        System.exit(100); // End the server after self test is complete
    }
}
