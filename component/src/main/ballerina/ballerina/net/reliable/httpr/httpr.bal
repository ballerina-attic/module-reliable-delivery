package ballerina.net.reliable.httpr;

import ballerina.util;
import ballerina.log;
import ballerina.net.http;
import ballerina.net.reliable.processor;
import ballerina.net.reliable;
import ballerina.net.reliable.storejms;

public const string MESSAGE_STORE_DESTINATION_NAME = "_BallerinaHTTPMessageStore_";

@Description { value:"Http client connector for reliable outbound HTTP requests"}
@Param { value:"serviceUri: Url of the service" }
@Param { value:"connectorOptions: connector options" }
@Param { value:"guaranteedProcessor: processor which responsible for handling guaranteed processing" }
public connector HttpGuaranteedClient (string serviceUri, http:Options connectorOptions, processor:GuaranteedProcessor guaranteedProcessor) {

    @Description { value:"GET action implementation of the reliable HTTP Connector"}
    @Param { value:"path: Request path" }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action get (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "get") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }

    @Description { value:"POST action implementation of the reliable HTTP Connector"}
    @Param { value:"path: Request path" }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action post (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "post") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }

    @Description { value:"HEAD action implementation of the reliable HTTP Connector"}
    @Param { value:"path: Request path" }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action head (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "head") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }

    @Description { value:"PUT action implementation of the reliable HTTP Connector"}
    @Param { value:"path: Request path" }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action put (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "put") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }

    @Description { value:"PATCH action implementation of the reliable HTTP Connector"}
    @Param { value:"path: Request path" }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action patch (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "patch") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }

    @Description { value:"DELETE action implementation of the reliable HTTP Connector"}
    @Param { value:"path: Request path" }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action delete (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "delete") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }

    @Description { value:"OPTIONS action implementation of the reliable HTTP Connector"}
    @Param { value:"path: Request path" }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action options (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "options") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }

    @Description { value:"forward action can be used to invoke an HTTP call with incoming request HTTPVerb"}
    @Param { value:"path: Request path" }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action forward (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "forward") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }

    @Description { value:"Invokes an HTTP call with the specified HTTP verb."}
    @Param { value:"HTTPVerb: HTTP verb value" }
    @Param { value:"path: Resource path " }
    @Param { value:"req: An HTTP Request struct" }
    @Return { value:"The response message indicates safe persistance of the incoming request" }
    @Return { value:"Error occured during HTTP reliable client storage action" }
    action execute (string HTTPVerb, string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, HTTPVerb) ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
}

public connector HttpClient (string serviceUri, http:Options connectorOptions) {

    action get (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "get") ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
    action post (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "post") ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
    action head (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "head") ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
    action put (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "put") ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
    action patch (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "patch") ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
    action delete (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "delete") ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
    action options (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "options") ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
    action forward (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "forward") ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
    action execute (string HTTPVerb, string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, HTTPVerb) ;
        return storeMessage(storableStream, generateMBConfiguration());
    }
}

function storeMessage(blob storableStream, processor:GuaranteedProcessor guaranteedProcessor)(http:Response, http:HttpConnectorError) {

    http:Response response = {};
    string destinationName = guaranteedProcessor.destinationName;

    // store the message
    try {
        transaction with retries(0) {
            guaranteedProcessor.store(guaranteedProcessor.config, destinationName, storableStream);
            log:printDebug("message successfully stored in persistent store");
            response.setJsonPayload({status:"Message successfully received into Ballerina"});
            response.setStatusCode(202);
        }
    } catch (error r) {
        log:printError("error when storing the message. " + r.msg);
        response.setJsonPayload({status:"Message failed to receive into Ballerina"});
        response.setStatusCode(503);
    }
    return response, null;
}

function convertToBlob(http:Request request, http:Options options, string serviceUri, string path, string httpMethod) (blob) {
    string encodedPayload = util:base64Encode(request.getStringPayload());
    reliable:HttpEPInvoke epInvoke = {request:request, serviceUrl:serviceUri, path:path, httpMethod:httpMethod, connectorOptions:options, payload:encodedPayload};
    var epInvokeJson,_ = <json> epInvoke;
    string epInvokeString = epInvokeJson.toString();
    blob serialized = epInvokeString.toBlob("UTF-8");
    return serialized;
}

public function handle (blob objectStream) (error) {
    endpoint<http:HttpClient> httpEp {
    }

    // build EPInvoke instance
    string epInvokeString = objectStream.toString("UTF-8");
    var epInvokeJson, ex = <json> epInvokeString;
    var epInvoke, er = <HttpEPInvoke> epInvokeJson;

    // access the request inside EPInvoke instance
    var request, _ = (http:Request) epInvoke.request;

    // Re-generate request headers from serialized request
    // Request headers are not getting de-serialized. Therefore we have to do that manually
    // All the maps are getting de-serialized as <string, any>. if the original map contained <string, struct>
    // Struct will not get de-serialized, it will stay as json. But ballerina think this is 'any' value, not json.
    // Due to that we can't cast it to json. (I have included a native function to do this for now)
    map requestHeaders = request.headers;
    string[] keys = requestHeaders.keys();

    request.removeAllHeaders();

    // copying headers
    int i = 0;
    while (i < lengthof keys) {
        json header = reliable:anyToJson(requestHeaders[keys[i]]);
        json firstElement = header[0];
        // ignoring params for the moment
        json value = firstElement["value"];
        request.addHeader(keys[i], value.toString());
        i =  i+1;
    }

    // Setting message content
    http:HeaderValue contentTypeHeader = request.getHeader("Content-Type");
    string contentType = contentTypeHeader.value;
    string payload = util:base64Decode(epInvoke.payload);
    error payloadError;
    
    
    if (contentType == "application/json") {
        json jsonPayload;
        jsonPayload, payloadError = <json> payload;
        request.setJsonPayload(jsonPayload);
    } else if (contentType == "application/xml") {
        xml xmlPayload;
        xmlPayload, payloadError = <xml>payload;
        request.setXmlPayload(xmlPayload);
    } else {
        request.setStringPayload(payload);
    }

    if (payloadError != null) {
        return payloadError;
    }

    // Invoking the backend
    error backendError = null;
    var options,_ = (http:Options) epInvoke.connectorOptions;
    http:HttpClient client = create http:HttpClient(epInvoke.serviceUrl, options);
    bind client with httpEp;

    http:Response response;
    http:HttpConnectorError connectorError;

    if ("get" == epInvoke.httpMethod) {
        response, connectorError = httpEp.get(epInvoke.path, request);
    } else if ("post" == epInvoke.httpMethod) {
        response, connectorError = httpEp.post(epInvoke.path, request);
    } else if ("head" == epInvoke.httpMethod) {
        response, connectorError = httpEp.head(epInvoke.path, request);
    } else if ("put" == epInvoke.httpMethod) {
        response, connectorError = httpEp.put(epInvoke.path, request);
    } else if ("patch" == epInvoke.httpMethod) {
        response, connectorError = httpEp.patch(epInvoke.path, request);
    } else if ("delete" == epInvoke.httpMethod) {
        response, connectorError = httpEp.delete(epInvoke.path, request);
    } else if ("options" == epInvoke.httpMethod) {
        response, connectorError = httpEp.options(epInvoke.path, request);
    } else if ("forward" == epInvoke.httpMethod) {
        response, connectorError = httpEp.forward(epInvoke.path, request);
    } else {
        response, connectorError = httpEp.execute(epInvoke.httpMethod, epInvoke.path, request);
    }

    if (connectorError != null) {
        backendError = {msg:"Backend invocation failed " + connectorError.msg};
    }

    println(response);

    return backendError;
}

function generateMBConfiguration() (processor:GuaranteedProcessor) {
    processor:GuaranteedProcessor guaranteedConfig = {
         config:{ "initialContextFactory":"org.apache.activemq.jndi.ActiveMQInitialContextFactory",
                    "providerUrl":"tcp://127.0.0.1:61616",
                    "connectionFactoryName":"QueueConnectionFactory"
                },
         store: storejms:store,
         retrieve: storejms:retrieve,
         handler: handle
     };
    _= guaranteedConfig.startProcessor();
    return guaranteedConfig;
}

struct HttpEPInvoke {
    string serviceUrl;
    string path;
    string httpMethod;
    Request request;
    Options connectorOptions;
    string payload;
}

struct Request {
    string remoteHost;
    int port;
    string path;
    string method;
    string httpVersion;
    string userAgent;
    map headers;
}

struct Options {
    int port;
    int endpointTimeout = 60000;
    boolean enableChunking = true;
    boolean keepAlive = true;
    FollowRedirects followRedirects;
    SSL ssl;
    Retry retryConfig;
    Proxy proxy;
}


struct Retry {
    int count;
    int interval;
}

struct SSL {
    string trustStoreFile;
    string trustStorePassword;
    string keyStoreFile;
    string keyStorePassword;
    string sslEnabledProtocols;
    string ciphers;
    string sslProtocol;
}

struct FollowRedirects {
    boolean enabled = false;
    int maxCount = 5;
}

struct Proxy {
    string host;
    int port;
    string userName;
    string password;
}
