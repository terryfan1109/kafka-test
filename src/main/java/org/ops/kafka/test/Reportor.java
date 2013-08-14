package org.ops.kafka.test;

import java.util.concurrent.atomic.AtomicLong;

public class Reportor {
  private AtomicLong counterOfSuccess = new AtomicLong(0);
  private AtomicLong counterOfFailure = new AtomicLong(0);

  public long addSuccess() {
    return counterOfSuccess.incrementAndGet();
  }

  public long addFailure() {
    return counterOfFailure.incrementAndGet();
  }

  public long getSuccess() {
    return counterOfSuccess.get();
  }

  public long getFailure() {
    return counterOfFailure.get();
  }
  
  public long getTotal() {
    return getSuccess() + getFailure();
  }
}
