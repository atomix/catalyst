/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.catalyst.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.atomix.catalyst.util.Assert.notNull;
import static java.util.stream.Collectors.toSet;

/**
 * A {@link ScheduledExecutorService} wrapper.
 * <p>
 * A {@link ScheduledExecutorService} wrapper that wraps instances of
 * {@link Runnable} and {@link Callable} with code that properly sets up the
 * ThreadLocal {@link #CONTEXT_THREAD_LOCAL} before transferring
 * control to the wrapped code.  The wrapper subsequently tears down the
 * ThreadLocal after the trapped code completes execution.
 * <p>
 * Note: All instances of {@link Runnable} and {@link Callable} *MUST* be
 * scheduled through instances of this class in order for Catalyst code
 * to work correctly.
 *
 * @author <a href="https://github.com/atomix/catalyst">Catalyst Project</a>
 */
class CatalystScheduledExecutorService implements ScheduledExecutorService {
  final static ThreadLocal<ThreadContext> CONTEXT_THREAD_LOCAL = new ThreadLocal<>();
  private final ScheduledExecutorService delegate;
  private final ThreadContext threadContext;

  /**
   * @param delegate      the wrapped instance of {@link ScheduledExecutorService}
   * @param threadContext the instance of {@link ThreadContext} to setup for scheduled tasks
   */
  CatalystScheduledExecutorService(ScheduledExecutorService delegate, ThreadContext threadContext) {
    this.delegate = notNull(delegate, "delegate");
    this.threadContext = notNull(threadContext, "threadContext");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
  {
    return delegate.schedule(new CatalystRunnable(command), delay, unit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
  {
    return delegate.schedule(new CatalystCallable<>(callable), delay, unit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
  {
    return delegate.scheduleAtFixedRate(new CatalystRunnable(command), initialDelay, period, unit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
  {
    return delegate.scheduleWithFixedDelay(new CatalystRunnable(command), initialDelay, delay, unit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  final public void shutdown() {
    delegate.shutdown();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  final public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  final public boolean isShutdown() {
    return delegate.isShutdown();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  final public boolean isTerminated() {
    return delegate.isTerminated();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  final public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegate.submit(new CatalystCallable<>(task));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegate.submit(new CatalystRunnable(task), result);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(new CatalystRunnable(task));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
  {
    Set<Callable<T>> wrapped = tasks.stream().map(CatalystCallable::new).collect(toSet());
    return delegate.invokeAll(wrapped);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                       long timeout, TimeUnit unit) throws InterruptedException
  {
    Set<Callable<T>> wrapped = tasks.stream().map(CatalystCallable::new).collect(toSet());
    return delegate.invokeAll(wrapped, timeout, unit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    Set<Callable<T>> wrapped = tasks.stream().map(CatalystCallable::new).collect(toSet());
    return delegate.invokeAny(wrapped);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                         long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                                                             TimeoutException
  {
    Set<Callable<T>> wrapped = tasks.stream().map(CatalystCallable::new).collect(toSet());
    return delegate.invokeAny(wrapped, timeout, unit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void execute(Runnable command) {
    delegate.execute(new CatalystRunnable(command));
  }

  /**
   * A Catalyst wrapper for instances of {@link Runnable}.
   */
  private class CatalystRunnable implements Runnable {
    private final Runnable delegate;

    CatalystRunnable(Runnable delegate) {
      this.delegate = notNull(delegate, "delegate");
    }

    @Override
    public void run() {
      CONTEXT_THREAD_LOCAL.set(threadContext);
      try {
        delegate.run();

      } catch (RejectedExecutionException ree) {
        throw ree;

      } catch (Throwable t) {
        threadContext.logger().error("An uncaught exception occurred", t);
        throw t;

      } finally {
        CONTEXT_THREAD_LOCAL.remove();
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CatalystRunnable that = (CatalystRunnable) o;
      return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }

  /**
   * A Catalyst wrapper for instances of {@link Callable}.
   */
  private class CatalystCallable<V> implements Callable<V> {
    private final Callable<V> delegate;

    CatalystCallable(Callable<V> delegate) {
      this.delegate = delegate;
    }

    @Override
    public V call() throws Exception {
      CONTEXT_THREAD_LOCAL.set(threadContext);
      try {
        return delegate.call();

      } catch (RejectedExecutionException ree) {
        throw ree;

      } catch (Throwable t) {
        threadContext.logger().error("An uncaught exception occurred", t);
        throw t;

      } finally {
        CONTEXT_THREAD_LOCAL.remove();
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CatalystCallable that = (CatalystCallable) o;
      return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }
}
