package io.atomix.catalyst.concurrent;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single threaded context.
 * <p>
 * This is a basic {@link ThreadContext} implementation that uses a
 * {@link java.util.concurrent.ScheduledExecutorService} to schedule events on the context thread.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SingleThreadContext implements ThreadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleThreadContext.class);
  private final ScheduledExecutorService executor;
  private final Serializer serializer;
  private volatile boolean blocked;
  private final Executor wrappedExecutor = new Executor() {
    @Override
    public void execute(Runnable command) {
      try {
        executor.execute(command);
      } catch (RejectedExecutionException e) {
      }
    }
  };

  /**
   * Creates a new single thread context.
   * <p>
   * The provided context name will be passed to {@link CatalystThreadFactory} and used
   * when instantiating the context thread.
   *
   * @param nameFormat The context nameFormat which will be formatted with a thread number.
   * @param serializer The context serializer.
   */
  public SingleThreadContext(String nameFormat, Serializer serializer) {
    this(new CatalystThreadFactory(nameFormat), serializer);
  }

  /**
   * Creates a new single thread context.
   *
   * @param factory The thread factory.
   * @param serializer The context serializer.
   */
  public SingleThreadContext(CatalystThreadFactory factory, Serializer serializer) {
    this(new ScheduledThreadPoolExecutor(1, factory), serializer);
  }

  /**
   * Creates a new single thread context.
   *
   * @param executor The executor on which to schedule events. This must be a single thread scheduled executor.
   * @param serializer The context serializer.
   */
  public SingleThreadContext(ScheduledExecutorService executor, Serializer serializer) {
    this.executor = new CatalystScheduledExecutorService(executor, this);
    this.serializer = serializer;
  }

  @Override
  public void block() {
    this.blocked = true;
  }

  @Override
  public void unblock() {
    this.blocked = false;
  }

  @Override
  public boolean isBlocked() {
    return blocked;
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
  public Executor executor() {
    return wrappedExecutor;
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable runnable) {
    ScheduledFuture<?> future = executor.schedule(runnable, delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public Scheduled schedule(Duration delay, Duration interval, Runnable runnable) {
    ScheduledFuture<?> future = executor.scheduleAtFixedRate(runnable, delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }

}
