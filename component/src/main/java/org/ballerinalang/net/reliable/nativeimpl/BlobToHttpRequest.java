/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.net.reliable.nativeimpl;

import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.ConnectorUtils;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.AbstractNativeFunction;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.http.Constants;
import org.ballerinalang.net.http.HttpUtil;
import org.ballerinalang.net.reliable.ReliableDeliveryConnectorUtils;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Ballerina native function to build http:Request from blob
 */
@BallerinaFunction(packageName = "ballerina.net.reliable",
                   functionName = "decodeHttpRequest",
                   args = {
                           @Argument(name = "binary stream",
                                     type = TypeKind.BLOB)
                   },
                   returnType = {
                           @ReturnType(type = TypeKind.STRUCT,
                                       structPackage = "ballerina.net.reliable",
                                       structType = "HttpEPInvoke")
                   },
                   isPublic = true)
public class BlobToHttpRequest extends AbstractNativeFunction {
    private static final Logger log = LoggerFactory.getLogger(BlobToHttpRequest.class);

    public BValue[] execute(Context ctx) {
        byte[] serializedData = this.getBlobArgument(ctx, 0);
        BStruct httpEpInvoke = ConnectorUtils.createAndGetStruct(ctx, "ballerina.net.reliable", "HttpEPInvoke");
        try {
            HttpActionInvokeInfo httpActionInvokeInfo = (HttpActionInvokeInfo) ReliableDeliveryConnectorUtils
                    .deserialize(serializedData);

            // create http:Request and add it to the holding struct
            BStruct request = ConnectorUtils
                    .createAndGetStruct(ctx, Constants.PROTOCOL_PACKAGE_HTTP, Constants.REQUEST);
            HttpUtil.addCarbonMsg(request, httpActionInvokeInfo.generateHttpRequest());
            httpEpInvoke.setRefField(0, request);

            httpEpInvoke.setStringField(0, httpActionInvokeInfo.getServiceUrl());
            httpEpInvoke.setStringField(1, httpActionInvokeInfo.getPath());
            httpEpInvoke.setStringField(2, httpActionInvokeInfo.getHttpMethod());
            httpEpInvoke.setStringField(3, httpActionInvokeInfo.getConnectorOptions());

        } catch (IOException | ClassNotFoundException e) {
            throw new BallerinaException("Error building struct from blob stream", e, ctx);
        }
        return this.getBValues(httpEpInvoke);
    }
}
