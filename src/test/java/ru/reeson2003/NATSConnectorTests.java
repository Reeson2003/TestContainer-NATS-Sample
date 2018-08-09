package ru.reeson2003;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.shaded.org.apache.commons.codec.binary.StringUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

@Slf4j
public class NATSConnectorTests {

    private static final String KEY = "Key";

    private static final String REQUEST = "Request";

    private static final String RESPONSE = "Response";

    public static final int PORT = 9876;

    public static final String URL = "nats://localhost:" + PORT;

    @Test
    public void sendTest()
            throws ExecutionException, InterruptedException {
        CompletableFuture.completedFuture(PORT)
                .thenApply(this::runNatsContainer)
                .thenApply(this::runTest)
                .thenAccept(GenericContainer::stop)
                .get();

    }

    private GenericContainer runTest(GenericContainer natsContainer) {
        try (Connection connection = Nats.connect(URL)) {
            connection.createDispatcher(msg -> connection.publish(msg.getReplyTo(), StringUtils.getBytesUtf8(RESPONSE)))
                    .subscribe(KEY);
            CompletableFuture<Message> messageFuture = (CompletableFuture<Message>) connection.request(KEY, StringUtils.getBytesUtf8(REQUEST));
            Thread.sleep(1000);

            String actual = messageFuture
                    .thenApply(msg -> StringUtils.newStringUtf8(msg.getData()))
                    .thenApply(text -> {
                        log.info("text = " + text);
                        return text;
                    })
                    .completeOnTimeout("Error", 1000, TimeUnit.MILLISECONDS)
                    .get();
            assertEquals(RESPONSE, actual);
        } finally {
            return natsContainer;
        }
    }

    private GenericContainer runNatsContainer(int port) {
        GenericContainer natsContainer;
        int containerExposedPort = 4222;
        Consumer<CreateContainerCmd> cmd = e -> e.withPortBindings(new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(containerExposedPort)));
        Consumer<OutputFrame> ofc = of -> log.info(of.getUtf8String());

        natsContainer = new GenericContainer("nats:latest")
                .withExposedPorts(containerExposedPort)
                .withLogConsumer(ofc)
                .withCreateContainerCmdModifier(cmd);
        CompletableFuture.runAsync(natsContainer::start);
        try {
            TimeUnit.SECONDS.sleep(1);
        } finally {
            return natsContainer;
        }
    }

}
