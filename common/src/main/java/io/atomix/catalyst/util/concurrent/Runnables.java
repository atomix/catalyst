package io.atomix.catalyst.util.concurrent;

import org.slf4j.Logger;

/**
 * Runnable utilities.
 */
final class Runnables {
  private Runnables() {
  }

  /**
   * Returns a wrapped runnable that logs and rethrows uncaught exceptions.
   */
  static Runnable logFailure(final Runnable runnable, Logger logger) {
    return () -> {
      try {
        runnable.run();
      } catch (Throwable t) {
        logger.error("An uncaught exception occurred", t);
        throw t;
      }
    };
  }
}
