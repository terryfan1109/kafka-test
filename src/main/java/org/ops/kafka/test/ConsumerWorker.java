package org.ops.kafka.test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class ConsumerWorker implements Runnable {
  AtomicBoolean exit = new AtomicBoolean(false);
  Consumer client;
  KafkaStream<byte[], byte[]> stream;
  BlockingQueue<Message> queue;
  Reportor reportor;
  int threadNo;

  public ConsumerWorker(KafkaStream<byte[], byte[]> stream,
      BlockingQueue<Message> queue, Reportor reportor, int threadNo) {
    this.stream = stream;
    this.queue = queue;
    this.reportor = reportor;
    this.threadNo = threadNo;
  }

  public void stop() {
    exit.set(true);
  }

  @Override
  public void run() {

    long count = 0;

    try {
      ConsumerIterator<byte[], byte[]> it = stream.iterator();

      while (!exit.get()) {
        Message msg = queue.poll(1, TimeUnit.SECONDS);
        while (null != msg) {
          try {            
            if (it.hasNext()) {
              MessageAndMetadata<byte[], byte[]> receivedMessage = it.next();

              if (receivedMessage.message().length > 0) {
                try {
                  System.out.println(new String(receivedMessage.message(), "UTF-8"));
                } catch (Throwable e) {
                  //... 
                }
                ++count;
                reportor.addSuccess();
              } else {
                reportor.addFailure();
              }
            }
          } catch (ConsumerTimeoutException e) {
            queue.put(msg);
            Thread.sleep(500);
          }
          msg = queue.poll(1, TimeUnit.SECONDS);
        }
      }
      
      System.out.println(String.format("Thread-%d received message #%d", threadNo, count));
      
    } catch (InterruptedException e) {
      // ...
      e.printStackTrace();
    }
  }
}
