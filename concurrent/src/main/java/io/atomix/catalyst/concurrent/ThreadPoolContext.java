/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.catalyst.concurrent;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Thread pool context.
 * <p>
 * This is a special {@link ThreadContext} implementation that schedules events to be executed
 * on a thread pool. Events executed by this context are guaranteed to be executed on order but may be executed on different
 * threads in the provided thread pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ThreadPoolContext implements ThreadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolContext.class);
  private final ScheduledExecutorService executor;
  private final Serializer serializer;
  private volatile boolean blocked;

  /**
   * Creates a new thread pool context.
   *
   * @param executor The thread pool on which to execute events.
   * @param serializer The context serializer.
   */
  public ThreadPoolContext(ScheduledExecutorService executor, Serializer serializer) {
    this.executor = new CatalystScheduledExecutorService(Assert.notNull(executor, "executor"), this);
    this.serializer = Assert.notNull(serializer, "serializer");
  }

  @Override
  public Logger logger() {
    return LOGGER;
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public boolean isBlocked() {
    return blocked;
  }

  @Override
  public void block() {
    blocked = true;
  }

  @Override
  public void unblock() {
    blocked = false;
  }

  @Override
  public Executor executor() {
    return executor;
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable runnable) {
    ScheduledFuture<?> future = executor.schedule(() -> executor.execute(runnable), delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public Scheduled schedule(Duration delay, Duration interval, Runnable runnable) {
    ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> executor.execute(runnable), delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public void close() {
    // Do nothing.
  }

}
