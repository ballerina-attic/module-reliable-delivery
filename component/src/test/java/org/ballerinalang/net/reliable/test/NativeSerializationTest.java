/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.ballerinalang.net.reliable.test;

import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.net.http.HttpUtil;
import org.ballerinalang.net.reliable.ReliableDeliveryConnectorUtils;
import org.ballerinalang.net.reliable.nativeimpl.HttpActionInvokeInfo;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.transport.http.netty.message.HTTPCarbonMessage;

import java.io.IOException;

import static org.ballerinalang.net.http.Constants.TRANSPORT_MESSAGE;

/**
 * Class to test Native Serialization and De-serializations.
 */
public class NativeSerializationTest {

    private CompileResult result;

    private String serviceUrl = "http://wso2.com";
    private String path = "/ballerina";
    private String method = "get";
    private String connectorOptions = "{\"optionParam\":\"optionValue\"}";

    @BeforeClass
    public void setup() {
        result = BCompileUtil.compile("bal-config/createAssert.bal");
    }

    @Test(description = "Test serialization and de-serialization native functions")
    public void serializationNativeTest() {

        BValue[] returns = BRunUtil.invoke(result, "createHttpRequest");
        BStruct httpRequest = (BStruct) returns[0];
        HTTPCarbonMessage httpCarbonMessage = (HTTPCarbonMessage) httpRequest.getNativeData(TRANSPORT_MESSAGE);

        HttpActionInvokeInfo storableMessage = new HttpActionInvokeInfo(httpCarbonMessage);
        storableMessage.setServiceUrl(serviceUrl);
        storableMessage.setPath(path);
        storableMessage.setHttpMethod(method);
        storableMessage.setConnectorOptions(connectorOptions);

        byte[] serializedContent;

        try {
            serializedContent = ReliableDeliveryConnectorUtils.serialize(storableMessage);
        } catch (IOException e) {
            throw new BallerinaException("test error when serializing the message struct");
        }

        Assert.assertTrue(serializedContent.length != 0, "error in http message serialization");

        // de-serialization

        HttpActionInvokeInfo httpActionInvokeInfo = null;
        try {
            httpActionInvokeInfo = (HttpActionInvokeInfo) ReliableDeliveryConnectorUtils.deserialize(serializedContent);
        } catch (IOException | ClassNotFoundException e) {
            throw new BallerinaException("test error when de-serializing the message struct");
        }

        Assert.assertNotNull(httpActionInvokeInfo, "error in http message de-serialization");

        Assert.assertEquals(httpActionInvokeInfo.getServiceUrl(), serviceUrl,
                "error in http service url serialization");
        Assert.assertEquals(httpActionInvokeInfo.getPath(), path, "error in http path serialization");
        Assert.assertEquals(httpActionInvokeInfo.getHttpMethod(), method, "error in http method serialization");
        Assert.assertEquals(httpActionInvokeInfo.getConnectorOptions(), connectorOptions,
                "error in http options serialization");

        BStruct deserializedRequest = (BStruct) (BRunUtil.invoke(result, "createEmptyHttpRequest")[0]);
        HttpUtil.addCarbonMsg(deserializedRequest, httpActionInvokeInfo.generateHttpRequest());

        //Assert message body and headers
        BValue[] args = { deserializedRequest };
        BBoolean isValid = (BBoolean) (BRunUtil.invoke(result, "assertResult", args)[0]);
        Assert.assertTrue(isValid.booleanValue(), "error in http body and headers in serialization");

    }
}
