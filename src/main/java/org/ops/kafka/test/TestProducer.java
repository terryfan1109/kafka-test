package org.ops.kafka.test;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.ops.kafka.test.Message;
import org.ops.kafka.test.Reportor;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class TestProducer {
  private int initWorkers = 1;
  private int maxWorkers = 64;
  private int numOfMessages = 0;
  private String brokerServer;
  private String topic;

  public TestProducer(int numOfMessage, int initWorkers, int maxWorkers,
      String brokerServer, String topic) {

    this.numOfMessages = numOfMessage;
    this.initWorkers = initWorkers;
    this.maxWorkers = maxWorkers;
    this.brokerServer = brokerServer;
    this.topic = topic;

  }

  public void execute() {
    Reportor reportor = new Reportor();
    ArrayBlockingQueue<Message> messageQueue = new ArrayBlockingQueue<Message>(
        maxWorkers);

    ProducerWorker[] workers = new ProducerWorker[maxWorkers];
    ArrayBlockingQueue<Runnable> workerQueue = new ArrayBlockingQueue<Runnable>(
        maxWorkers);
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(initWorkers,
        maxWorkers, 60, TimeUnit.SECONDS, workerQueue);

    Properties props = new Properties();
    props.put("metadata.broker.list", brokerServer);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    props.put("producer.type", "sync");

    ProducerConfig config = new ProducerConfig(props);

    for (int i = 0; i < maxWorkers; ++i) {
      Producer<String, String> client = new Producer<String, String>(config);
      ProducerWorker worker = new ProducerWorker(client, messageQueue, reportor);
      workers[i] = worker;
    }

    for (int i = 0; i < workers.length; ++i) {
      threadPool.execute(workers[i]);
    }

    long start = System.currentTimeMillis();
    Random randGen = new Random();
    byte[] randMessage = new byte[2 * 1024];
    for (int i = 0; i < numOfMessages; ++i) {
      randGen.nextBytes(randMessage);
      String msg = Base64.encodeBase64URLSafeString(randMessage);

      try {
        messageQueue.put(new Message(topic, msg));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("signal workers to stop");

    for (int i = 0; i < workers.length; ++i) {
      workers[i].stop();
    }

    System.out.println("shutdown workers");

    try {
      threadPool.shutdown();
      threadPool.awaitTermination(86400, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // ...
    }

    long elapse = System.currentTimeMillis() - start;

    System.out.println(String.format(
        "Complete : rps=%f (in %d ms), success=%d, failure=%d",
        (numOfMessages * 1000.0) / elapse, elapse, reportor.getSuccess(),
        reportor.getFailure()));
  }
}


