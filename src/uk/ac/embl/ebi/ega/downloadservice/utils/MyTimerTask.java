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

import java.text.NumberFormat;
import java.util.TimerTask;

/**
 *
 * @author asenf
 * 
 * Perform Periodic Maintenance tasks - TODO
 * 
 */
public class MyTimerTask extends TimerTask {
    
    @Override
    public void run() {
        System.gc();
        
        Runtime runtime = Runtime.getRuntime();

        NumberFormat format = NumberFormat.getInstance();

        StringBuilder sb = new StringBuilder();
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();

        sb.append("free memory: ").append(format.format(freeMemory / 1024)).append("\n");
        sb.append("allocated memory: ").append(format.format(allocatedMemory / 1024)).append("\n");
        sb.append("max memory: ").append(format.format(maxMemory / 1024)).append("\n");
        sb.append("total free memory: ").append(format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024)).append("<br/>");
        
        System.out.println(sb.toString());
    }
    
}
