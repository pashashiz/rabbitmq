package com.ps.tutorial;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class PublishSubscribeExample {

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        int logsCount = 10;
        CountDownLatch latch = new CountDownLatch(1);
        executor.execute(new Publisher(logsCount, latch));
        executor.execute(new Subscriber("1", latch));
        executor.execute(new Subscriber("2", latch));
        executor.execute(new Subscriber("3", latch));
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    public static class Publisher implements Runnable {

        private final Logger log = LogManager.getLogger(getClass());
        private final int logsCount;
        private final CountDownLatch latch;

        public Publisher(int logsCount, CountDownLatch latch) {
            this.logsCount = logsCount;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                log.debug("Started");
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                try (Connection connection = factory.newConnection()) {
                    Channel channel = connection.createChannel();
                    channel.exchangeDeclare("logs", "fanout");
                    IntStream.range(1, logsCount + 1).forEach(i ->{
                        try {
                            channel.basicPublish("logs", "", null, ("log-" + i).getBytes());
                            log.debug("Sent a log: log-" + i);
                            Thread.sleep(100);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    latch.countDown();
                    channel.close();
                    log.debug("Exit");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Subscriber implements Runnable {

        private final Logger log = LogManager.getLogger(getClass());
        private final String name;
        private final CountDownLatch latch;

        public Subscriber(String name, CountDownLatch latch) {
            this.name = name;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                log.debug("Started");
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                try (Connection connection = factory.newConnection()) {
                    Channel channel = connection.createChannel();
                    channel.exchangeDeclare("logs", "fanout");
                    String tempQueueName = channel.queueDeclare().getQueue();
                    channel.queueBind(tempQueueName, "logs", "");
                    log.debug("Waiting for tasks...");
                    channel.basicConsume(tempQueueName, true, new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            String logMessage = new String(body);
                            log.debug("[" + name + "] received a log message: " + logMessage);
                        }
                    });
                    latch.await();
                    log.debug("Exit");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
