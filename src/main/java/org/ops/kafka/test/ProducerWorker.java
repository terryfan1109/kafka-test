package org.ops.kafka.test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.ops.kafka.test.Message;
import org.ops.kafka.test.Reportor;

public class ProducerWorker implements Runnable {
  AtomicBoolean exit = new AtomicBoolean(false);
  Producer<String, String> client;
  BlockingQueue<Message> queue;
  Reportor reportor;

  public ProducerWorker(Producer<String, String> client,
      BlockingQueue<Message> queue, Reportor reportor) {
    this.client = client;
    this.queue = queue;
    this.reportor = reportor;
  }

  public void stop() {
    exit.set(true);
  }

  @Override
  public void run() {
    long count = 0;
    
    try {
      while (!exit.get()) {
        Message msg = queue.poll(1, TimeUnit.SECONDS);
        while (null != msg) {
          try {
            KeyedMessage<String, String> message = new KeyedMessage<String, String>(
                msg.topic, msg.body);

            client.send(message);
            ++count;
            if (0 == count % 1000) {
              System.out.print(".");
            }
            reportor.addSuccess();

            msg = queue.poll(1, TimeUnit.SECONDS);
          } catch (Throwable e) {
            e.printStackTrace();
            reportor.addFailure();
          }
        }
      }
    } catch (InterruptedException e) {
      // ...
      e.printStackTrace();
    }
  }
}