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

package uk.ac.embl.ebi.ega.downloadservice.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.OverlappingFileLockException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author asenf
 * 
 * Write text messages to a file, with time stamp. A new file is created every day after midnight.
 */
public class Dailylog {
    
    private String logfilename;
    private Date dt;
    private String dateString;
    private int day;
    
    private File the_log;
    private String the_path;
    
    public Dailylog(String logname) {
        // Start logging. folder name will be /xferlog
        //                filename will be <given>_<date>.txt
        // New log file every 24 h (1 per day)
        
        dt = new Date();
        SimpleDateFormat parserSDF=new SimpleDateFormat("EE MMM dd HH:mm:ss yyyy");
        dateString = parserSDF.format(dt);
        String ip = "";
        try {
            ip = InetAddress.getLocalHost().toString();
            if (ip.contains(".")) ip = ip.substring(0, ip.indexOf('.')).trim();
        } catch (UnknownHostException t) {;}

        logfilename = logname + "-" + ip;
        if (logfilename.contains("/")) logfilename = logfilename.replaceAll("/", "_");
        day = Integer.parseInt(dateString.substring(8,10).trim());
        
        SimpleDateFormat sdf_new = new SimpleDateFormat("yyMMdd");
        dateString = sdf_new.format(dt);
        day = Integer.parseInt(dateString.substring(4,6).trim());
        
        String path = "./dailylog/";
        File path_test = new File(path);
        if (!path_test.exists()) {
            path_test.mkdir();
        }
        
        String log = logname;
        log = log + "-" + dateString + "-000000";
        
        String filepath = "";
        try {
            filepath = path_test.getCanonicalPath() + "/" + log;
            the_path = path_test.getCanonicalPath() + "/";
        } catch (IOException ex) {
            Logger.getLogger(Dailylog.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        the_log = new File(filepath);
        if (!the_log.exists()) {
            try {
                the_log.createNewFile();
            } catch (IOException ex) {
                Logger.getLogger(Dailylog.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    // current-time   transfer-time   remote-host    file-size   filename    transfer-type   special-action-flag   direction  access-mode    username    ser\xadvice-name    authentication-method  authenticated-user-id   completion-status
    public synchronized void log(String line) {
        //System.out.println("Attempting to Log");
        
        dt = new Date();
        
        SimpleDateFormat sdf_new = new SimpleDateFormat("yyMMdd");
        dateString = sdf_new.format(dt);
        day = Integer.parseInt(dateString.substring(4,6).trim());
        
        String log = logfilename;
        log = log + "-" + dateString + "-000000";
        String filepath = the_path + log;

        // See if a new file is needed
        the_log = new File(filepath);
        if (!the_log.exists()) {
            try {
                the_log.createNewFile();
            } catch (IOException ex) {
                Logger.getLogger(Dailylog.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        // Create log entry
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");
        Date currentDate = new Date(System.currentTimeMillis());
        String current_time = sdf.format(currentDate);
        
        String ip = "";
        try {
            ip = InetAddress.getLocalHost().toString() + " ";
        } catch (UnknownHostException t) {;}
        
        String log_entry = current_time + " " + ip +
                           line;
        
        // Open the log file for appending, and write to the log
        try {
            FileOutputStream fos = new FileOutputStream(the_log, true);
            PrintStream ps = new PrintStream(fos);
            boolean written = false;
            do {
                try {
//                    // Lock it!
//                    FileLock lock = fos.getChannel().lock();
//                    try {
                        // Write the bytes.
                        ps.println(log_entry);
                        ps.flush();
                        written = true;
//                    } finally {
//                        // Release the lock.
//                        lock.release();
//                    }
                } catch ( OverlappingFileLockException ofle ) {
                    try {
                        // Wait a bit
                        Thread.sleep(5);
                    } catch (InterruptedException ex) {
                        throw new InterruptedIOException ("Interrupted waiting for a file lock.");
                    }
                }
            } while (!written);
            fos.close();
            //System.out.println("*** Log Success");
        } catch (IOException e) {
            System.out.println("*** Log Error: " + e.toString());
        }        
    }    
}
