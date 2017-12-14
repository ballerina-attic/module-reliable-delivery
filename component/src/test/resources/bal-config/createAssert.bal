import ballerina.net.http;

const string payload = "My sample payload";

const string headerName = "sampleHeaderName";

const string headerValue = "sampleHeaderValue";

function createHttpRequest () (http:Request) {
    http:Request request = {};
    request.setStringPayload(payload);
    request.setHeader(headerName, headerValue);
    return request;
}

function createEmptyHttpRequest () (http:Request) {
    http:Request request = {};
    return request;
}

function assertResult (http:Request request) (boolean) {
    if (request.getHeader(headerName) != headerValue || request.getStringPayload() != payload) {
        return false;
    }
    return true;
}
