package com.getindata.tutorial.generator;

import com.getindata.tutorial.avro.LogEvent;

import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;

/**
 * Factory class which creates infinite stream of LogEvent records
 */
public class LogEventStreamFactory {

  private static final int WAIT_TIME_MILLIS = 500;

  public static Stream<LogEvent> get() {
    final Iterator<LogEvent> iterator = new LogEventInfiniteInterator(new LogEventGenerator(), WAIT_TIME_MILLIS);
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, IMMUTABLE | NONNULL), false);
  }

}