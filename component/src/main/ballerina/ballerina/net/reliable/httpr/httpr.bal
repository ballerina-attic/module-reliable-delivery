package ballerina.net.reliable.httpr;

import ballerina.log;
import ballerina.net.reliable.processor;
import ballerina.net.reliable;
import ballerina.net.http;
import ballerina.net.reliable.storejms;

public const string MESSAGE_STORE_DESITNATION_NAME = "_BallerinaHTTPMessageStore_";

public connector HttpGuaranteedClient (string serviceUri, http:Options connectorOptions, processor:GuaranteedProcessor guaranteedProcessor) {

    action get (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "get") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
    action post (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "post") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
    action head (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "head") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
    action put (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "put") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
    action patch (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "patch") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
    action delete (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "delete") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
    action options (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "options") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
    action forward (string path, http:Request req) (http:Response, http:HttpConnectorError) {
        blob storableStream = convertToBlob(req, connectorOptions, serviceUri, path, "forward") ;
        return storeMessage(storableStream, guaranteedProcessor);
    }
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
        transaction {
            guaranteedProcessor.store(guaranteedProcessor.config, destinationName, storableStream);
            log:printDebug("[MP] message successfully stored in persistent store");
            response.setJsonPayload({status:"Message successfully received into Ballerina"});
            response.setStatusCode(202);
        } failed {
            retry 0;
        }
    } catch (error r) {
        log:printError("[MP] error when stroing the message. " + r.msg);
        response.setJsonPayload({status:"Message failed to receive into Ballerina"});
        response.setStatusCode(503);
    }
    return response, null;
}

function convertToBlob(http:Request request, http:Options options, string serviceUri, string path, string httpMethod) (blob) {
    var optionsJSON,_ = <json> options;
    reliable:HttpEPInvoke epInvoke = {request:request, serviceUrl:serviceUri, path:path, httpMethod:httpMethod, connectorOptions:optionsJSON.toString()};
    blob serialized = epInvoke.encodeToBlob();
    return serialized;
}

public function handle (blob objectStream) (error){
    endpoint<http:HttpClient> httpEp {
    }

    reliable:HttpEPInvoke epInvoke = reliable:decodeHttpRequest(objectStream);
    http:Request request = epInvoke.request;
    error backendError = null;

    // reconstruct http:Options
    string optionsString = epInvoke.connectorOptions;
    var optionsJSON,_= <json> optionsString;
    var options,_ = <http:Options> optionsJSON;

    http:HttpClient client = create http:HttpClient(epInvoke.serviceUrl, options);
    bind client with httpEp;

    http:Response response;
    http:HttpConnectorError e;

    // todo: remove this log
    log:printDebug("[MP] request payload: " + request.getStringPayload());

    if ("get" == epInvoke.httpMethod) {
        response, e = httpEp.get(epInvoke.path, request);
    } else if ("post" == epInvoke.httpMethod) {
        response, e = httpEp.post(epInvoke.path, request);
    } else if ("head" == epInvoke.httpMethod) {
        response, e = httpEp.head(epInvoke.path, request);
    } else if ("put" == epInvoke.httpMethod) {
        response, e = httpEp.put(epInvoke.path, request);
    } else if ("patch" == epInvoke.httpMethod) {
        response, e = httpEp.patch(epInvoke.path, request);
    } else if ("delete" == epInvoke.httpMethod) {
        response, e = httpEp.delete(epInvoke.path, request);
    } else if ("options" == epInvoke.httpMethod) {
        response, e = httpEp.options(epInvoke.path, request);
    } else if ("forward" == epInvoke.httpMethod) {
        response, e = httpEp.forward(epInvoke.path, request);
    } else {
        response, e = httpEp.execute(epInvoke.httpMethod, epInvoke.path, request);
    }

    if (e != null) {
        backendError = {msg:"Backend invocation failed " + e.msg};
    } else {
        // todo: remove this log
        log:printDebug("[MP] response from backend: " + response.getStringPayload());
    }

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