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

import org.ballerinalang.model.util.StringUtils;
import org.ballerinalang.model.util.XMLUtils;
import org.ballerinalang.model.values.BJSON;
import org.ballerinalang.model.values.BXML;
import org.ballerinalang.net.http.Constants;
import org.ballerinalang.net.http.HttpUtil;
import org.ballerinalang.runtime.message.BallerinaMessageDataSource;
import org.ballerinalang.runtime.message.StringDataSource;
import org.wso2.transport.http.netty.message.HTTPCarbonMessage;
import org.wso2.transport.http.netty.message.HttpMessageDataStreamer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Holder to store information regarding http endpoint invocation.
 * <p>
 * This will hold following information.
 *  - request body.
 *  - request headers.
 *  - service url.
 *  - path.
 *  - action of the HttpClient.
 *  - options of the connector.
 *
 */
public class HttpActionInvokeInfo implements Serializable {

    private String serviceUrl;

    private String path;

    private String httpMethod;

    private String connectorOptions;

    private Map<String, String> headers = new HashMap<>();

    private byte[] messageBody;

    /**
     * Generate {@link HttpActionInvokeInfo} for a particular {@link HTTPCarbonMessage}
     *
     * @param httpCarbonMessage input http content
     */
    public HttpActionInvokeInfo(HTTPCarbonMessage httpCarbonMessage) {
        //process carbon message and build the storable message

        // i. Store message headers
        httpCarbonMessage.getHeaders().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));

        // ii. Store message body
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        if (!httpCarbonMessage.isAlreadyRead()) {
            buildHttpMessage(httpCarbonMessage);
        }
        ((BallerinaMessageDataSource) httpCarbonMessage.getMessageDataSource()).setOutputStream(outputStream);
        httpCarbonMessage.getMessageDataSource().serializeData();
        messageBody = outputStream.toByteArray();

    }

    private void buildHttpMessage(HTTPCarbonMessage httpCarbonMessage) {
        String contentType = httpCarbonMessage.getHeader(Constants.CONTENT_TYPE);

        if (Constants.APPLICATION_JSON.equals(contentType)) {
            BJSON result = new BJSON(new HttpMessageDataStreamer(httpCarbonMessage).getInputStream());
            httpCarbonMessage.waitAndReleaseAllEntities();
            httpCarbonMessage.setMessageDataSource(result);
            result.setOutputStream(new HttpMessageDataStreamer(httpCarbonMessage).getOutputStream());
            httpCarbonMessage.setAlreadyRead(true);
        } else if (Constants.APPLICATION_XML.equals(contentType)) {
            BXML result = XMLUtils.parse(new HttpMessageDataStreamer(httpCarbonMessage).getInputStream());
            httpCarbonMessage.waitAndReleaseAllEntities();
            httpCarbonMessage.setMessageDataSource(result);
            result.setOutputStream(new HttpMessageDataStreamer(httpCarbonMessage).getOutputStream());
            httpCarbonMessage.setAlreadyRead(true);
        } else {
            // todo: handle empty string payloads
            String payload = StringUtils
                    .getStringFromInputStream(new HttpMessageDataStreamer(httpCarbonMessage).getInputStream());
            httpCarbonMessage.waitAndReleaseAllEntities();
            httpCarbonMessage.setMessageDataSource(new StringDataSource(payload));
            httpCarbonMessage.setAlreadyRead(true);
        }
    }

    /**
     * Generate a {@link HTTPCarbonMessage} using the content populated in the this instance
     *
     * @return {@link HTTPCarbonMessage}
     */
    public HTTPCarbonMessage generateHttpRequest() {
        HTTPCarbonMessage httpCarbonMessage = HttpUtil.createHttpCarbonMessage(true);

        // i. setting the message headers
        headers.forEach((key, value) -> httpCarbonMessage.setHeader(key, value));

        // ii. setting the message body (Datasource)
        BallerinaMessageDataSource payload = null;
        String contentType = headers.get(Constants.CONTENT_TYPE);

        if (Constants.APPLICATION_JSON.equals(contentType)) {
            payload = new BJSON(new ByteArrayInputStream(messageBody));
        } else if (Constants.APPLICATION_XML.equals(contentType)) {
            payload = XMLUtils.parse(new ByteArrayInputStream(messageBody));
        } else {
            payload = new StringDataSource(new String(messageBody));
        }
        payload.setOutputStream(new HttpMessageDataStreamer(httpCarbonMessage).getOutputStream());

        httpCarbonMessage.waitAndReleaseAllEntities();

        httpCarbonMessage.setMessageDataSource(payload);
        httpCarbonMessage.setAlreadyRead(true);

        return httpCarbonMessage;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
    }

    public void setConnectorOptions(String connectorOptions) {
        this.connectorOptions = connectorOptions;
    }

    public String getConnectorOptions() {
        return connectorOptions;
    }
}
