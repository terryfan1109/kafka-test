package org.ops.kafka.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class TestConsumer {
  private int maxWorkers = 64;
  private int numOfMessages = 0;
  private String zookeeperServers;
  private String topic;
  private String groupId;

  public TestConsumer(int numOfMessage, int maxWorkers,
      String zookeeperServers, String topic, String groupId) {

    this.numOfMessages = numOfMessage;
    this.maxWorkers = maxWorkers;
    this.zookeeperServers = zookeeperServers;
    this.topic = topic;
    this.groupId = groupId;

  }

  public void execute() {
    Reportor reportor = new Reportor();
    long elapse = 0;

    {
      Properties props = new Properties();
      props.put("zookeeper.connect", zookeeperServers);
      props.put("group.id", null == groupId ? 0: groupId);
      props.put("zookeeper.session.timeout.ms", "400");
      props.put("zookeeper.sync.time.ms", "200");
      props.put("auto.commit.interval.ms", "1000");
  
      ConsumerConfig config = new ConsumerConfig(props);
      ConsumerConnector client = Consumer.createJavaConsumerConnector(config);
  
      Map<String, Integer> topicWorkersMap = new HashMap<String, Integer>();
      topicWorkersMap.put(topic, new Integer(maxWorkers));
  
      Map<String, List<KafkaStream<byte[], byte[]>>> streamsMap = client
          .createMessageStreams(topicWorkersMap);
  
      List<KafkaStream<byte[], byte[]>> streams = streamsMap.get(topic);
  
      System.out.println(String.format("maximum workers %d for topic - %s",
          streams.size(), topic));
  
      ConsumerWorker[] workers = new ConsumerWorker[streams.size()];
  
      ArrayBlockingQueue<Runnable> workerQueue = new ArrayBlockingQueue<Runnable>(
          workers.length);
  
      ThreadPoolExecutor threadPool = new ThreadPoolExecutor(workers.length,
          workers.length, 60, TimeUnit.SECONDS, workerQueue);
  
      ArrayBlockingQueue<Message> messageQueue = new ArrayBlockingQueue<Message>(
          workers.length);
  
      for (int i = 0; i < workers.length; ++i) {
        ConsumerWorker worker = new ConsumerWorker(streams.get(i), messageQueue,
            reportor);
        workers[i] = worker;
      }
  
      for (int i = 0; i < workers.length; ++i) {
        threadPool.execute(workers[i]);
      }
  
      long start = System.currentTimeMillis();
      for (int i = 0; i < numOfMessages; ++i) {
        try {
          messageQueue.put(new Message(topic, null));
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
        threadPool.shutdownNow();
      } catch (InterruptedException e) {
        // ...
        e.printStackTrace();
      }
  
      elapse = System.currentTimeMillis() - start;

      client.shutdown(); 
    }
    
    System.out.println(String.format(
        "Complete : rps=%f (in %d ms), success=%d, failure=%d",
        (numOfMessages * 1000.0) / elapse, elapse, reportor.getSuccess(),
        reportor.getFailure()));
  }

}