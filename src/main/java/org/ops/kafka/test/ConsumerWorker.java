package org.ops.kafka.test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class ConsumerWorker implements Runnable {
  AtomicBoolean exit = new AtomicBoolean(false);
  Consumer client;
  KafkaStream<byte[], byte[]> stream;
  BlockingQueue<Message> queue;
  Reportor reportor;

  public ConsumerWorker(KafkaStream<byte[], byte[]> stream,
      BlockingQueue<Message> queue, Reportor reportor) {
    this.stream = stream;
    this.queue = queue;
    this.reportor = reportor;
  }

  public void stop() {
    exit.set(true);
  }

  @Override
  public void run() {

    try {
      ConsumerIterator<byte[], byte[]> it = stream.iterator();

      while (!exit.get()) {
        Message msg = queue.poll(1, TimeUnit.SECONDS);
        System.out.print(".");
        while (null != msg) {

          try {
            if (it.hasNext()) {
              MessageAndMetadata<byte[], byte[]> receivedMessage = it.next();
              if (receivedMessage.message().length > 0) {
                reportor.addSuccess();
              } else {
                reportor.addFailure();
              }
            } else {
              System.out.println("no message");
              reportor.addFailure();
            }

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
    
    System.out.println("exit");
  }
}