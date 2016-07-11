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

import static uk.ac.embl.ebi.ega.downloadservice.EgaSecureDownloadService.restCall;
import us.monoid.web.JSONResource;
import us.monoid.web.Resty;

/**
 *
 * @author asenf
 * 
 * Purely to enable a limited load test scenario in the service self-test
 */
public class MyLoadTest implements Runnable {
    
    private final Resty r;
    private final String query;
    private final int index;
    private long delta = 0;

    public MyLoadTest(String query, int index, Resty r) {
        this.r = r;
        this.query = query; 
        this.index = index;
    }
    public MyLoadTest(String query, int index) {
        this.r = new Resty();
        this.query = query; 
        this.index = index;
    }
    
    @Override
    public void run() {
        this.delta = System.currentTimeMillis();
        JSONResource json = restCall(r, query, null, null);
        assert(json!=null);
        this.delta = System.currentTimeMillis() - this.delta;
    }
    
    public long getDelta() {
        return this.delta;
    }
}
