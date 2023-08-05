/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.example;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.AbstractEventListener;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.NamingEvent;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.api.common.Constants.DEFAULT_GROUP;

/**
 * Nacos naming fuzzy subscribe example.
 * <p>Add the JVM parameter to run the NamingExample:</p>
 * {@code -DserverAddr=${nacos.server.ip}:${nacos.server.port} -Dnamespace=${namespaceId}}
 *
 * @author tanyongquan
 */
public class NamingFuzzySubscribeExample {
    
    public static void main(String[] args) throws NacosException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", System.getProperty("serverAddr", "localhost"));
        properties.setProperty("namespace", System.getProperty("namespace", "public"));
        
        NamingService naming = NamingFactory.createNamingService(properties);
        
        naming.registerInstance("nacos.test.1", "11.11.11.11", 8888);
        naming.registerInstance("nacos.test.2", "11.11.11.11", 8888);
        naming.registerInstance("nacos.test.3", "11.11.11.11", 8888);
        naming.registerInstance("nacos.otherName.1", "11.11.11.11", 8888);
        naming.registerInstance("nacos.otherGroup.1", "Group2", "11.11.11.11", 8888);
        
        Executor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("test-thread");
                    return thread;
                });
        
        naming.fuzzySubscribe("nacos.test", DEFAULT_GROUP, new AbstractEventListener() {
            
            @Override
            public Executor getExecutor() {
                return executor;
            }
            
            @Override
            public void onEvent(Event event) {
                System.out.println("Fuzzy subscribe with prefix service name" + ((NamingEvent) event).getInstances());
            }
        });
        
        Thread.sleep(1000);
        
        naming.registerInstance("nacos.test.4", "11.11.11.11", 8888);
        
        Thread.sleep(1000);
        
        naming.registerInstance("nacos.otherName.2", "11.11.11.11", 8888);
        
        Thread.sleep(1000);
        
        naming.registerInstance("nacos.otherGroup.2", "Group2", "11.11.11.11", 8888);
        
        Thread.sleep(1000);
        
        naming.deregisterInstance("nacos.test.1", "11.11.11.11", 8888);
        naming.deregisterInstance("nacos.test.2", "11.11.11.11", 8888);
        naming.deregisterInstance("nacos.test.3", "11.11.11.11", 8888);
        naming.deregisterInstance("nacos.otherName.1", "11.11.11.11", 8888);
        naming.deregisterInstance("nacos.otherGroup.1", "Group2", "11.11.11.11", 8888);
        naming.deregisterInstance("nacos.test.4", "11.11.11.11", 8888);
        naming.deregisterInstance("nacos.otherName.2", "11.11.11.11", 8888);
        naming.deregisterInstance("nacos.otherGroup.2", "Group2", "11.11.11.11", 8888);
    }
}
