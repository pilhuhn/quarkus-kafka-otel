package de.bsd.quarkus_kafka_otel;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.reactive.messaging.TracingMetadata;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 *
 */
@ApplicationScoped
public class KafkaReceiver {

    @Inject
    @Channel("topic1")
    Emitter<String> emitter;

    @Incoming("topic2")
    CompletionStage<Void> process(Message<String> message) {

        String body = message.getPayload();

        // Get the tracing header
        Optional<TracingMetadata> optionalTracingMetadata = TracingMetadata.fromMessage(message);
        if (optionalTracingMetadata.isPresent()) {
            TracingMetadata tracingMetadata = optionalTracingMetadata.get();

            // Take the trace info from the header
            try (Scope scope = tracingMetadata.getCurrentContext().makeCurrent()) {

                System.out.println("TraceId " + Span.current().getSpanContext().getTraceId());

                // Process again if needed
                if (shouldForwardAgain(body)) {

                    // We need to use a Message to emit
                    Message<String> out = Message.of(body);

                    // Add the tracing metadata to the outgoing message header
                    out = out.addMetadata(TracingMetadata.withCurrent(Context.current()));

                    // And send to round 2
                    emitter.send(out);
                }
            }
        }
        return message.ack();
    }

    private static boolean shouldForwardAgain(String body) {
        // If the message is "Hello World  from Python  from Python" then we have seen it already
        return body.contains("P") && body.indexOf("P") == body.lastIndexOf("P");
    }
}
