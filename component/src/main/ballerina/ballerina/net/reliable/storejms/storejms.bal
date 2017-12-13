package ballerina.net.reliable.storejms;

import ballerina.net.jms;

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

@Description {value:"Get object content of the JMS message"}
@Return {value:"any: Object Message Content"}
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
                                          connectionFactoryType:"queue",
                                          acknowledgementMode:jms:SESSION_TRANSACTED
                                      };
    return properties;
}
