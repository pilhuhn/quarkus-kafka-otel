package de.bsd.quarkus_kafka_otel;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;

/**
 *
 */
@ApplicationScoped
public class KafkaReceiver {

    @Incoming("topic2")
    void process(String message) {

        System.out.println("Got a message: " + message);

    }
}
