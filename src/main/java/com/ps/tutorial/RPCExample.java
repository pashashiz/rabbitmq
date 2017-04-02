package com.ps.tutorial;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;

public class RPCExample {

    private static final Logger log = LogManager.getLogger(RPCExample.class);

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        RPCEchoServer server = new RPCEchoServer();
        executor.execute(server);
        Thread.sleep(100);
        RPCClient client = new RPCClient();
        String response = client.send("Hello");
        log.debug("Response: " + response);
        client.close();
        server.close();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    public static class RPCClient {

        private final Logger log = LogManager.getLogger(getClass());
        private final Connection connection;
        private final Channel channel;
        private final String replyQueueName;

        public RPCClient() {
            log.debug("Started RPC echo client");
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                connection = factory.newConnection();
                channel = connection.createChannel();
                replyQueueName = channel.queueDeclare().getQueue();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void close() {
            try {
                channel.close();
                connection.close();
                log.debug("Closed");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String send(String message) {
            try {
                log.debug("Sending a message: " + message);
                String correlationId = UUID.randomUUID().toString();
                AMQP.BasicProperties properties = new AMQP.BasicProperties()
                        .builder()
                        .replyTo(replyQueueName)
                        .correlationId(correlationId)
                        .build();
                channel.basicPublish("", "rpc", properties, message.getBytes());
                BlockingQueue<String> exchanger = new SynchronousQueue<>();
                channel.basicConsume(replyQueueName, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                       try {
                           if (properties.getCorrelationId().equals(correlationId)) {
                               log.debug("Received a response: " + new String(body));
                               exchanger.offer(new String(body));
                           }
                       } catch (Exception e){
                           log.error(e);
                           exchanger.offer(e.getMessage());
                       }
                    }
                });
                return exchanger.take();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class RPCEchoServer implements Runnable {

        private final Logger log = LogManager.getLogger(getClass());
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run() {
            try {
                log.debug("Started RPC echo service");
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                try (Connection connection = factory.newConnection()) {
                    Channel channel = connection.createChannel();
                    channel.queueDeclare("rpc", false, false, false, null);
                    channel.basicQos(1);
                    log.debug("Waiting for requests...");
                    channel.basicConsume("rpc", false, new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            try {
                                log.debug("Received a request: " + new String(body));
                                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                                        .Builder()
                                        .correlationId(properties.getCorrelationId())
                                        .build();
                                channel.basicPublish("", properties.getReplyTo(), replyProps, ("Echo: " + new String(body)).getBytes());
                            } finally {
                                channel.basicAck(envelope.getDeliveryTag(), false);
                            }
                        }
                    });
                    latch.await();
                    log.debug("Closed");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void close() {
            latch.countDown();
        }
    }

}
