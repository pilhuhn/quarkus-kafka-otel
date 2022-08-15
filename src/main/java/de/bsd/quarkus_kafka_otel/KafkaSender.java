package de.bsd.quarkus_kafka_otel;


import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 *
 */
@ApplicationScoped
@Path("/send")
public class KafkaSender {

    @Inject
    @Channel("topic1")
    Emitter<String> emitter;

    @GET
    public String doSend() {
        emitter.send("Hello World");
        return "done";
    }
}
