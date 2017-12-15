package ballerina.net.reliable.storejms;

import ballerina.net.jms;

@Description {value:"Store blob stream to the jms destination"}
@Param {value:"config: jms client configuration"}
@Param {value:"queue: jms destination name"}
@Param {value:"objectStream: blob stream"}
public function store (map config, string queue, blob objectStream) {
    endpoint<jms:JmsClient> jmsEP {
    }

    jms:ClientProperties properties = generateClientConfiguration(config);

    jms:JmsClient jmsConnector = create jms:JmsClient(properties);
    bind jmsConnector with jmsEP;

    jms:JMSMessage message = jms:createBytesMessage(properties);
    message.setBytesMessageContent(objectStream);
    jmsEP.send(queue, message);
}

@Description {value:"Retrieve blob stream from the jms destination"}
@Param {value:"config: jms client configuration"}
@Param {value:"queue: jms destination name"}
@Return {value:"objectStream: blob content of the jms message"}
public function retrieve (map config, string queue) (blob objectStream) {
    endpoint<jms:JmsClient> jmsEP {
    }

    jms:JmsClient jmsConnector = create jms:JmsClient(generateClientConfiguration(config));
    bind jmsConnector with jmsEP;

    jms:JMSMessage message = jmsEP.poll(queue, 1000);

    if (message == null) {
        string empty = "";
        return empty.toBlob("UTF-8");
    }

    return message.getBytesMessageContent();
}

function generateClientConfiguration(map configurationMap) (jms:ClientProperties) {

    // This method is created because Ballerina map to struct conversions are not working for struts in a
    // different package
    var initialContextFactoryValue,_ = (string) configurationMap["initialContextFactory"];
    var providerUrlValue,_ = (string) configurationMap["providerUrl"];
    var connectionFactoryNameValue,_ = (string) configurationMap["connectionFactoryName"];

    jms:ClientProperties properties = {initialContextFactory:initialContextFactoryValue,
                                          providerUrl:providerUrlValue,
                                          connectionFactoryName:connectionFactoryNameValue,
                                          connectionFactoryType:jms:TYPE_QUEUE,
                                          acknowledgementMode:jms:SESSION_TRANSACTED
                                      };
    return properties;
}
