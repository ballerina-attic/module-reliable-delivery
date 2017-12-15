package ballerina.net.reliable.processor;

import ballerina.log;
import ballerina.task;

public map retryCounterMap = {};

@Description { value:"Struct representing a message processor in Ballerina" }
@Field {value:"retryCount: number of time the retrying should happen before giving up"}
@Field {value:"interval: interval from one message consume iteration to another"}
@Field {value:"config: Configuration needed to connect to the message store"}
@Field {value:"store: function pointer to handle storing task"}
@Field {value:"retrieve: function pointer to handle retrival task"}
@Field {value:"handler: function pointer to handle retrieved message"}
public struct GuaranteedProcessor {
    int retryCount = 5;
    int interval = 5000;
    map config;
    function(map, string, blob) store;
    function(map, string)(blob) retrieve;
    function(blob) (error) handler;
    string destinationName = "_BallerinaHTTPMessageStore_";
    string taskId;
}

public GuaranteedProcessor guaranteedProcessor =  null;

@Description {value:"Start the message processor. This need to be called once per-ballerina startup"}
@Param {value:"processor: Message processor instance"}
@Return {value:"boolean: Whether the message processor successfully started or not"}
public function <GuaranteedProcessor processor> startProcessor () (boolean){

    // if a processor has already registered, ignore this call
    // we need to have collection of processors but it is blocking on the feature of function pointer closures
    if (guaranteedProcessor != null) {
        log:printWarn("[MP] processor is already registered and running");
        return false;
    }
    guaranteedProcessor = processor;

    //start the task
    function () returns (error) onTriggerFunction = handleMessage;

    function (error e) onErrorFunction = cleanupError;

    var taskId, schedulerError = task:scheduleTimer(onTriggerFunction,
                                                    onErrorFunction, {delay:1000, interval:guaranteedProcessor.interval});
    guaranteedProcessor.taskId = taskId;
    retryCounterMap[taskId] = guaranteedProcessor.retryCount;
    if (schedulerError != null) {
        log:printError("[MP] task schduling failed: " + schedulerError.msg) ;
    } else {
        log:printDebug("[MP] task schduled with ID: " + taskId);
    }
    return true;
}

function handleMessage() returns (error) {
    int retryIteration;
    retryIteration,_ = (int) retryCounterMap[guaranteedProcessor.taskId];

    string destinationName = guaranteedProcessor.destinationName;

    blob retrievedMessageStream;
    transaction {
        retrievedMessageStream = guaranteedProcessor.retrieve(guaranteedProcessor.config, destinationName);
        string empty = "";
        if (retrievedMessageStream.toString("UTF-8") != empty) {
            log:printDebug("[MP] message received at the processor");
            //retrying is over
            if(retryIteration == 0) {
                retryCounterMap[guaranteedProcessor.taskId] = guaranteedProcessor.retryCount;
                log:printDebug("[MP] moving message to dlc due to max retry exceeded");
                guaranteedProcessor.store(guaranteedProcessor.config, destinationName+"_dlc", retrievedMessageStream);
            } else {
                error e = guaranteedProcessor.handler(retrievedMessageStream);
                if (e != null) {
                    log:printDebug("[MP] endpoint invocation failed for " + (guaranteedProcessor.retryCount - retryIteration + 1) + " iteration");
                    retryCounterMap[guaranteedProcessor.taskId] =  retryIteration-1;
                    abort;
                }
            }
        }
        retryCounterMap[guaranteedProcessor.taskId] = guaranteedProcessor.retryCount;
    } failed {
        retry 0;
    }
    return null;
}

function cleanupError(error e) {
    retryCounterMap[guaranteedProcessor.taskId] = guaranteedProcessor.retryCount;
}