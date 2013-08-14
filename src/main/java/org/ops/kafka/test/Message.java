package org.ops.kafka.test;

public class Message {
  public String topic;
  public String body;

  public Message(String topic, String body) {
    this.topic = topic;
    this.body = body;
  }
}
