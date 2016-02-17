package io.atomix.catalyst.util.concurrent;

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
    this(getThread(executor), executor, serializer);
  }

  public SingleThreadContext(Thread thread, ScheduledExecutorService executor, Serializer serializer) {
    this.executor = executor;
    if (executor instanceof ScheduledThreadPoolExecutor) {
      ((ScheduledThreadPoolExecutor) executor).setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
    }
    this.serializer = serializer;
    Assert.state(thread instanceof CatalystThread, "not a Catalyst thread");
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
    executor.shutdownNow();
  }

}
