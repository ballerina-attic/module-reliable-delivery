package ballerina.net.reliable;

import ballerina.net.http;

public struct HttpEPInvoke {
    http:Request request;
    string serviceUrl;
    string path;
    string httpMethod;
    string connectorOptions;
}

public native function <HttpEPInvoke request> encodeToBlob () (blob);

public native function decodeHttpRequest (blob stream) (HttpEPInvoke);
