package com.getindata.tutorial.generator;

import com.getindata.tutorial.avro.LogEvent;

import java.util.Iterator;

final class LogEventInfiniteInterator implements Iterator<LogEvent> {

  private final LogEventGenerator generator;
  private final int waitTimeMillis;

  public LogEventInfiniteInterator(LogEventGenerator generator, int waitTimeMillis) {
    this.generator = generator;
    this.waitTimeMillis = waitTimeMillis;
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public LogEvent next() {
    waitSomeTime();
    return generator.generate();
  }

  private void waitSomeTime() {
    try {
      Thread.sleep(waitTimeMillis);
    } catch (InterruptedException e) {
      // swallow
    }
  }
}
