package ballerina.net.reliable;

import ballerina.net.http;

public struct HttpEPInvoke {
    string serviceUrl;
    string path;
    string httpMethod;
    http:Request request;
    http:Options connectorOptions;
    string payload;
}

public native function anyToJson (any anyValue)(json);
