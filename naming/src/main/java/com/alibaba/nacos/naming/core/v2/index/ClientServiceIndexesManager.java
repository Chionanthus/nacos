/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.core.v2.index;

import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.trace.DeregisterInstanceReason;
import com.alibaba.nacos.common.trace.event.naming.DeregisterInstanceTraceEvent;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client and service index manager.
 *
 * @author xiweng.yy
 */
@Component
public class ClientServiceIndexesManager extends SmartSubscriber {
    
    private final ConcurrentMap<Service, Set<String>> publisherIndexes = new ConcurrentHashMap<>();
    
    private final ConcurrentMap<Service, Set<String>> subscriberIndexes = new ConcurrentHashMap<>();
    
    /**
     * service -> Set{match fuzzySubscribePattern}.
     */
    private final ConcurrentMap<Service, Set<String>> fuzzySubscribeMatchIndexes = new ConcurrentHashMap<>();
    
    /**
     * fuzzySubscribePattern -> Set{clientID}.
     */
    private final ConcurrentMap<String, Set<String>> fuzzySubscriberIndexes = new ConcurrentHashMap<>();
    
    public ClientServiceIndexesManager() {
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
    }
    
    public Collection<String> getAllClientsRegisteredService(Service service) {
        return publisherIndexes.containsKey(service) ? publisherIndexes.get(service) : new ConcurrentHashSet<>();
    }
    
    public Collection<String> getAllClientsSubscribeService(Service service) {
        return subscriberIndexes.containsKey(service) ? subscriberIndexes.get(service) : new ConcurrentHashSet<>();
    }
    
    public Collection<Service> getSubscribedService() {
        return subscriberIndexes.keySet();
    }
    
    public Collection<String> getFuzzySubscribeMatchIndexes(Service service) {
        return fuzzySubscribeMatchIndexes.containsKey(service)
                ? fuzzySubscribeMatchIndexes.get(service) : new ConcurrentHashSet<>();
    }
    
    public Collection<String> getAllFuzzySubscribePattern() {
        return fuzzySubscriberIndexes.keySet();
    }
    
    public Collection<String> getAllClientFuzzySubscribe(String pattern) {
        return fuzzySubscriberIndexes.containsKey(pattern) ? fuzzySubscriberIndexes.get(pattern) : new ConcurrentHashSet<>();
    }
    
    /**
     * Clear the service index without instances.
     *
     * @param service The service of the Nacos.
     */
    public void removePublisherIndexesByEmptyService(Service service) {
        if (publisherIndexes.containsKey(service) && publisherIndexes.get(service).isEmpty()) {
            publisherIndexes.remove(service);
            fuzzySubscribeMatchIndexes.remove(service);
        }
    }
    
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        result.add(ClientOperationEvent.ClientRegisterServiceEvent.class);
        result.add(ClientOperationEvent.ClientDeregisterServiceEvent.class);
        result.add(ClientOperationEvent.ClientSubscribeServiceEvent.class);
        result.add(ClientOperationEvent.ClientUnsubscribeServiceEvent.class);
        result.add(ClientOperationEvent.ClientFuzzySubscribeEvent.class);
        result.add(ClientOperationEvent.ClientCancelFuzzySubscribeEvent.class);
        result.add(ClientOperationEvent.ClientReleaseEvent.class);
        return result;
    }
    
    @Override
    public void onEvent(Event event) {
        if (event instanceof ClientOperationEvent.ClientReleaseEvent) {
            handleClientDisconnect((ClientOperationEvent.ClientReleaseEvent) event);
        } else if (event instanceof ClientOperationEvent) {
            handleClientOperation((ClientOperationEvent) event);
        }
    }
    
    private void handleClientDisconnect(ClientOperationEvent.ClientReleaseEvent event) {
        Client client = event.getClient();
        for (Service each : client.getAllSubscribeService()) {
            removeSubscriberIndexes(each, client.getClientId());
        }
        for (String eachPattern : client.getAllFuzzySubscribePattern()) {
            removeFuzzySubscriberIndexes(eachPattern, client.getClientId());
        }
        DeregisterInstanceReason reason = event.isNative()
                ? DeregisterInstanceReason.NATIVE_DISCONNECTED : DeregisterInstanceReason.SYNCED_DISCONNECTED;
        long currentTimeMillis = System.currentTimeMillis();
        for (Service each : client.getAllPublishedService()) {
            removePublisherIndexes(each, client.getClientId());
            InstancePublishInfo instance = client.getInstancePublishInfo(each);
            NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(currentTimeMillis,
                    "", false, reason, each.getNamespace(), each.getGroup(), each.getName(),
                    instance.getIp(), instance.getPort()));
        }
    }
    
    private void handleClientOperation(ClientOperationEvent event) {
        Service service = event.getService();
        String clientId = event.getClientId();
        if (event instanceof ClientOperationEvent.ClientRegisterServiceEvent) {
            addPublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientDeregisterServiceEvent) {
            removePublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientSubscribeServiceEvent) {
            addSubscriberIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientUnsubscribeServiceEvent) {
            removeSubscriberIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientFuzzySubscribeEvent) {
            String completedPattern = ((ClientOperationEvent.ClientFuzzySubscribeEvent) event).getFuzzySubscribePattern();
            addFuzzySubscriberIndexes(completedPattern, clientId);
        } else if (event instanceof ClientOperationEvent.ClientCancelFuzzySubscribeEvent) {
            String completedPattern = ((ClientOperationEvent.ClientCancelFuzzySubscribeEvent) event).getFuzzySubscribePattern();
            removeFuzzySubscriberIndexes(completedPattern, clientId);
        }
    }
    
    private void addPublisherIndexes(Service service, String clientId) {
        if (!publisherIndexes.containsKey(service)) {
            // The only time the index needs to be updated is when the service is first created
            updateFuzzySubscriptionIndex(service);
        }
        publisherIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
        publisherIndexes.get(service).add(clientId);
        NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
    }
    
    private void removePublisherIndexes(Service service, String clientId) {
        publisherIndexes.computeIfPresent(service, (s, ids) -> {
            ids.remove(clientId);
            NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
            return ids.isEmpty() ? null : ids;
        });
    }
    
    private void addSubscriberIndexes(Service service, String clientId) {
        subscriberIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
        // Fix #5404, Only first time add need notify event.
        if (subscriberIndexes.get(service).add(clientId)) {
            NotifyCenter.publishEvent(new ServiceEvent.ServiceSubscribedEvent(service, clientId));
        }
    }
    
    private void removeSubscriberIndexes(Service service, String clientId) {
        if (!subscriberIndexes.containsKey(service)) {
            return;
        }
        subscriberIndexes.get(service).remove(clientId);
        if (subscriberIndexes.get(service).isEmpty()) {
            subscriberIndexes.remove(service);
        }
    }
    
    private void addFuzzySubscriberIndexes(String completedPattern, String clientId) {
        fuzzySubscriberIndexes.computeIfAbsent(completedPattern, key -> new ConcurrentHashSet<>());
        if (fuzzySubscriberIndexes.get(completedPattern).add(clientId)) {
            Collection<Service> matchedService = updateFuzzySubscriptionIndex(completedPattern);
            NotifyCenter.publishEvent(new ServiceEvent.FuzzySubscribeEvent(clientId, completedPattern, matchedService));
        }
        System.out.println("exist-fuzzy-pattern:" + fuzzySubscriberIndexes.keySet());
    }
    
    private void removeFuzzySubscriberIndexes(String completedPattern, String clientId) {
        if (!fuzzySubscriberIndexes.containsKey(completedPattern)) {
            return;
        }
        fuzzySubscriberIndexes.get(completedPattern).remove(clientId);
        if (fuzzySubscriberIndexes.get(completedPattern).isEmpty()) {
            fuzzySubscriberIndexes.remove(completedPattern);
        }
    }
    
    /**
     * This method will build/update the match index of fuzzySubscription.
     *
     * @param service The service of the Nacos.
     */
    public void updateFuzzySubscriptionIndex(Service service) {
        Set<String> filteredPattern = NamingUtils.filterPatternWithNamespace(service.getNamespace(), fuzzySubscriberIndexes.keySet());
        Set<String> matchedPattern = NamingUtils.getServiceMatchedPatterns(service.getName(), service.getGroup(),
                filteredPattern);
        if (CollectionUtils.isNotEmpty(matchedPattern)) {
            fuzzySubscribeMatchIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
            for (String each : matchedPattern) {
                fuzzySubscribeMatchIndexes.get(service).add(NamingUtils.getCompletedPattern(service.getNamespace(), each));
            }
        }
    }
    
    /**
     * This method will build/update the match index of fuzzySubscription.
     *
     * @param completedPattern the completed pattern of fuzzy subscribe (with namespace id).
     * @return Updated set of services that can match this pattern.
     */
    public Collection<Service> updateFuzzySubscriptionIndex(String completedPattern) {
        String namespaceId = NamingUtils.getNamespaceId(completedPattern);
        String pattern = NamingUtils.getPattern(completedPattern);
        Collection<Service> serviceSet = ServiceManager.getInstance().getSingletons(namespaceId);
        Set<Service> matchedService = new HashSet<>();
        for (Service service : serviceSet) {
            String serviceName = service.getName();
            String groupName = service.getGroup();
            String serviceNamePattern = NamingUtils.getServiceName(pattern);
            String groupNamePattern = NamingUtils.getGroupName(pattern);
            if (NamingUtils.isMatchPattern(serviceName, groupName, serviceNamePattern, groupNamePattern)) {
                fuzzySubscribeMatchIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
                fuzzySubscribeMatchIndexes.get(service).add(completedPattern);
                matchedService.add(service);
            }
        }
        return matchedService;
    }
    
    /**
     * This method will remove the match index of fuzzySubscription.
     *
     * @param service The service of the Nacos.
     * @param matchedPattern the pattern to remove
     */
    public void removeFuzzySubscriptionMatchIndex(Service service, String matchedPattern) {
        if (!fuzzySubscribeMatchIndexes.containsKey(service)) {
            return;
        }
        fuzzySubscribeMatchIndexes.get(service).remove(matchedPattern);
        if (fuzzySubscribeMatchIndexes.get(service).isEmpty()) {
            fuzzySubscribeMatchIndexes.remove(service);
        }
    }
}
