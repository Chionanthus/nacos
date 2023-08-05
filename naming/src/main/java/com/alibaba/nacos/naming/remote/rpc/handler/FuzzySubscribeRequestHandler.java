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

package com.alibaba.nacos.naming.remote.rpc.handler;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.remote.NamingRemoteConstants;
import com.alibaba.nacos.api.naming.remote.request.FuzzySubscribeRequest;
import com.alibaba.nacos.api.naming.remote.response.FuzzySubscribeResponse;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.naming.core.v2.service.impl.EphemeralClientOperationServiceImpl;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.plugin.auth.constant.ActionTypes;
import org.springframework.stereotype.Component;

/**
 * Fuzzy subscribe request handler.
 *
 * @author tanyongquan
 */
@Component("fuzzySubscribeRequestHandler")
public class FuzzySubscribeRequestHandler extends RequestHandler<FuzzySubscribeRequest, FuzzySubscribeResponse> {
    
    private final EphemeralClientOperationServiceImpl clientOperationService;
    
    public FuzzySubscribeRequestHandler(EphemeralClientOperationServiceImpl clientOperationService) {
        this.clientOperationService = clientOperationService;
    }
    
    @Override
    @Secured(action = ActionTypes.READ)
    public FuzzySubscribeResponse handle(FuzzySubscribeRequest request, RequestMeta meta) throws NacosException {
        String serviceNamePattern = request.getServiceName();
        String groupNamePattern = request.getGroupName();
        String namespaceId = request.getNamespace();
        String app = request.getHeader("app", "unknown");
        String groupedServicePattern = NamingUtils.getGroupedName(serviceNamePattern, groupNamePattern);
        Subscriber fuzzySubscriber = new Subscriber(meta.getClientIp(), meta.getClientVersion(), app, meta.getClientIp(),
                namespaceId, groupedServicePattern, 0, "");
        switch (request.getType()) {
            case NamingRemoteConstants.FUZZY_SUBSCRIBE_SERVICE:
                // TODO
                // clientOperationService.fuzzySubscribeService(namespaceId, serviceNamePattern, groupNamePattern,
                //        fuzzySubscriber, meta.getConnectionId());
                return FuzzySubscribeResponse.buildSuccessResponse(NamingRemoteConstants.FUZZY_SUBSCRIBE_SERVICE);
            case NamingRemoteConstants.CANCEL_FUZZY_SUBSCRIBE_SERVICE:
                // TODO
                // clientOperationService.cancelFuzzySubscribeService(namespaceId, serviceNamePattern, groupNamePattern,
                //         fuzzySubscriber, meta.getConnectionId());
                return FuzzySubscribeResponse.buildSuccessResponse(NamingRemoteConstants.CANCEL_FUZZY_SUBSCRIBE_SERVICE);
            default:
                throw new NacosException(NacosException.INVALID_PARAM,
                        String.format("Unsupported request type %s", request.getType()));
        }
    }
}
