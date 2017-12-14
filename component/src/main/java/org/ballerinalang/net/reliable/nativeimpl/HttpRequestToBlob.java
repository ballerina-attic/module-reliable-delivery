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
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BBlob;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.AbstractNativeFunction;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.reliable.ReliableDeliveryConnectorUtils;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.transport.http.netty.message.HTTPCarbonMessage;

import java.io.IOException;

import static org.ballerinalang.net.http.Constants.TRANSPORT_MESSAGE;

/**
 * Ballerina native function to encode http:Request to blob.
 */
@BallerinaFunction(packageName = "ballerina.net.reliable",
                   functionName = "encodeToBlob",
                   returnType = {
                           @ReturnType(type = TypeKind.BLOB)
                   },
                   receiver = @Receiver(type = TypeKind.STRUCT,
                                        structType = "HttpEPInvoke",
                                        structPackage = "ballerina.net.reliable"),
                   isPublic = true)
public class HttpRequestToBlob extends AbstractNativeFunction {
    private static final Logger log = LoggerFactory.getLogger(HttpRequestToBlob.class);

    public BValue[] execute(Context ctx) {
        BStruct epInvokeInfo = ((BStruct) this.getRefArgument(ctx, 0));

        BStruct httpRequest = (BStruct) epInvokeInfo.getRefField(0);
        HTTPCarbonMessage httpCarbonMessage = (HTTPCarbonMessage) httpRequest.getNativeData(TRANSPORT_MESSAGE);

        String serviceUrl = epInvokeInfo.getStringField(0);
        String path = epInvokeInfo.getStringField(1);
        String httpMethod = epInvokeInfo.getStringField(2);

        String connectorOptions = epInvokeInfo.getStringField(3);

        // build the serializable object
        HttpActionInvokeInfo storableMessage = new HttpActionInvokeInfo(httpCarbonMessage);
        storableMessage.setServiceUrl(serviceUrl);
        storableMessage.setPath(path);
        storableMessage.setHttpMethod(httpMethod);
        storableMessage.setConnectorOptions(connectorOptions);

        try {
            byte[] serializedContent = ReliableDeliveryConnectorUtils.serialize(storableMessage);
            BBlob bBlob = new BBlob(serializedContent);
            return this.getBValues(bBlob);
        } catch (IOException e) {
            throw new BallerinaException("error when serializing the message struct", ctx);
        }
    }
}
