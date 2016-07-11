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

package uk.ac.embl.ebi.ega.downloadservice;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.udt.UdtChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.util.HashMap;
import uk.ac.embl.ebi.ega.downloadservice.endpoints.Service;

/**
 *
 * @author asenf
 * 
 * Initialize Pipeline for UDT Traffic; otherwise identical to TCP Pipeline
 */
public class EgaSecureDownloadServiceInitializerUDT extends ChannelInitializer<UdtChannel> {

    private final SslContext sslCtx;
    private final HashMap<String, Service> endpointMappings;
    private final DefaultEventExecutorGroup l, s, loc; // long and short lasting requests handled separately
    private final GlobalTrafficShapingHandler globalTrafficShapingHandler;
    
    private final EgaSecureDownloadService ref;
    
    public EgaSecureDownloadServiceInitializerUDT(SslContext sslCtx, HashMap mappings, 
            DefaultEventExecutorGroup l, DefaultEventExecutorGroup s,
            GlobalTrafficShapingHandler globalTrafficShapingHandler,
            EgaSecureDownloadService ref) {
        this.sslCtx = sslCtx;
        this.endpointMappings = mappings;
        this.l = l;
        this.s = s;
        this.loc = new DefaultEventExecutorGroup(5);
        this.globalTrafficShapingHandler = globalTrafficShapingHandler;
        
        this.ref = ref;
    }
    
    @Override
    protected void initChannel(UdtChannel ch) throws Exception {
        // Add SSL handler first to encrypt and decrypt everything.
        // In this example, we use a bogus certificate in the server side
        // and accept any invalid certificates in the client side.
        // You will need something more complicated to identify both
        // and server in the real world.
        if (sslCtx != null) {
            ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()));
        }
        
        // Count (potentially: shape) traffic
        ch.pipeline().addLast(globalTrafficShapingHandler);
        
        // On top of the SSL handler, add the text line codec.
        ch.pipeline().addLast(new HttpServerCodec());
        ch.pipeline().addLast(new HttpObjectAggregator(65536));
        ch.pipeline().addLast(new ChunkedWriteHandler());

        ch.pipeline().addLast(this.loc, new EgaChannelSelector(this.sslCtx!=null,
                                              this.endpointMappings,
                                              this.s, this.l,
                                              this.ref));
        
        // and then business logic. This is CPU heavy - place in separate thread
        //ch.pipeline().addLast(this.e, new EgaSecureDownloadServiceHandler(this.sslCtx!=null,
        //                                                           this.endpointMappings));

    }

}