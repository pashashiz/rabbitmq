package com.ps.tutorial;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class WorkQueueExample {

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        int tasksCount = 10;
        executor.execute(new Sender(tasksCount));
        CountDownLatch latch = new CountDownLatch(tasksCount);
        executor.execute(new Receiver("1", latch));
        executor.execute(new Receiver("2", latch));
        executor.execute(new Receiver("3", latch));
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    public static class Sender implements Runnable {

        private final Logger log = LogManager.getLogger(getClass());
        private final int tasksCount;

        public Sender(int tasksCount) {
            this.tasksCount = tasksCount;
        }

        @Override
        public void run() {
            try {
                log.debug("Started");
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                try (Connection connection = factory.newConnection()) {
                    Channel channel = connection.createChannel();
                    // idempotent, the queue gets created only if doesn't exist
                    channel.queueDeclare("tasks", false, false, false, null);
                    IntStream.range(1, tasksCount + 1).forEach(i -> {
                        try {
                            channel.basicPublish("", "tasks", null, ("task-" + i).getBytes());
                            log.debug("Sent a task: task-" + i);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    channel.close();
                    log.debug("Exit");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Receiver implements Runnable {

        private final Logger log = LogManager.getLogger(getClass());
        private final String name;
        private final CountDownLatch latch;

        public Receiver(String name, CountDownLatch latch) {
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
                    channel.basicQos(1);
                    // idempotent, the queue gets created only if doesn't exist
                    channel.queueDeclare("tasks", false, false, false, null);
                    log.debug("Waiting for tasks...");
                    channel.basicConsume("tasks", false, new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            try {
                                String task = new String(body);
                                log.debug("[" + name + "] received a task: " + task + ", processing...");
                                Thread.sleep(1000);
                                log.debug("[" + name + "] processed a task: " + task + ", processing...");
                                latch.countDown();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            } finally {
                                // here we manually say the broker that message was delivered and processed
                                channel.basicAck(envelope.getDeliveryTag(), false);
                            }
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
