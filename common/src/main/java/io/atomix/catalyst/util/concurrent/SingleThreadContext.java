package io.atomix.catalyst.util.concurrent;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.catalyst.serializer.Serializer;

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
  private final Executor wrappedExecutor = new Executor() {
    @Override
    public void execute(Runnable command) {
      executor.execute(Runnables.logFailure(command, LOGGER));
    }
  };

  /**
   * Creates a new single thread context.
   * <p>
   * The provided context name will be passed to {@link CatalystThreadFactory} and used
   * when instantiating the context thread.
   *
   * @param name The context name.
   * @param serializer The context serializer.
   */
  public SingleThreadContext(String name, Serializer serializer) {
    this(Executors.newSingleThreadScheduledExecutor(new CatalystThreadFactory(name)), serializer);
  }

  /**
   * Creates a new single thread context.
   *
   * @param executor The executor on which to schedule events. This must be a single thread scheduled executor.
   * @param serializer The context serializer.
   */
  public SingleThreadContext(ScheduledExecutorService executor, Serializer serializer) {
    this(getThread(executor), executor, serializer);
  }

  public SingleThreadContext(Thread thread, ScheduledExecutorService executor, Serializer serializer) {
    this.executor = executor;
    this.serializer = serializer;
    if (!(thread instanceof CatalystThread)) {
      throw new IllegalStateException("not a Catalyst thread");
    }
    ((CatalystThread) thread).setContext(this);
  }

  /**
   * Gets the thread from a single threaded executor service.
   */
  protected static CatalystThread getThread(ExecutorService executor) {
    final AtomicReference<CatalystThread> thread = new AtomicReference<>();
    try {
      executor.submit(() -> {
        thread.set((CatalystThread) Thread.currentThread());
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize thread state", e);
    }
    return thread.get();
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
    ScheduledFuture<?> future = executor.schedule(Runnables.logFailure(runnable, LOGGER), delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public Scheduled schedule(Duration delay, Duration interval, Runnable runnable) {
    ScheduledFuture<?> future = executor.scheduleAtFixedRate(Runnables.logFailure(runnable, LOGGER), delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public void close() {
    executor.shutdown();
  }

}
