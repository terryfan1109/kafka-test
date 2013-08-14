package org.ops.kafka;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.ops.kafka.test.TestConsumer;
import org.ops.kafka.test.TestProducer;

/**
 * Hello world!
 * 
 */
public class App {
  public static void main(String[] args) {
    Options supportedOptions = new Options()
        .addOption("h", "help", false, "Display help")
        .addOption("k", "kafka", true, "Kafka broker servers")
        .addOption("z", "zookeeper", true, "Zookeeper servers")
        .addOption("t", "queue", true, "Kafka topic")
        .addOption("g", "groupId", true, "Kafka consumer group id")
        .addOption("x", "action", true, "Action, enqueue or dequeue")
        .addOption("i", "init_workers", true, "Initial workers")
        .addOption("m", "max_workers", true, "Maximum workers")
        .addOption("n", "number_messages", true, "The Number of messages");

    // parse arguments
    CommandLine cmd = null;

    try {
      CommandLineParser parser = new GnuParser();
      cmd = parser.parse(supportedOptions, args);
      if (!cmd.hasOption("h")) {
        if ("enqueue".equalsIgnoreCase(cmd.getOptionValue("x"))) {
          enqueue_test(cmd);
        } else if ("dequeue".equalsIgnoreCase(cmd.getOptionValue("x"))) {
          dequeue_test(cmd);
        } else {
          show_usage(supportedOptions);
        }
      } else {
        show_usage(supportedOptions);
      }
    } catch (UnrecognizedOptionException e) {
      System.out.println(e.getMessage());
      show_usage(supportedOptions);
    } catch (Throwable e) {
      System.out.println(e.getMessage());
    }
  }

  private static void show_usage(Options options) {
    HelpFormatter help = new HelpFormatter();
    help.printHelp("Kafka Test Producer", options);
  }

  private static void enqueue_test(CommandLine cmd) {

    String broker = cmd.getOptionValue("k");
    String topic = cmd.getOptionValue("t");
    int initWorkers = Integer.parseInt(cmd.getOptionValue("i", "1"));
    int maxWorkers = Integer.parseInt(cmd.getOptionValue("m", "5"));
    int numOfMessage = Integer.parseInt(cmd.getOptionValue("n", "10"));

    TestProducer producer = new TestProducer(numOfMessage, initWorkers,
        maxWorkers, broker, topic);
    producer.execute();
  }

  private static void dequeue_test(CommandLine cmd) {

    String zookeepers = cmd.getOptionValue("z");
    String topic = cmd.getOptionValue("t");
    String groupId = cmd.getOptionValue("g", null);
    int maxWorkers = Integer.parseInt(cmd.getOptionValue("m", "5"));
    int numOfMessage = Integer.parseInt(cmd.getOptionValue("n", "10"));

    TestConsumer consumer = new TestConsumer(numOfMessage, maxWorkers,
        zookeepers, topic, groupId);
    consumer.execute();
  }

}
