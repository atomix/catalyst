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
package io.atomix.catalyst.buffer;

import java.io.File;
import java.nio.channels.FileChannel;

/**
 * Direct {@link java.nio.ByteBuffer} based buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MappedBuffer extends ByteBufferBuffer {

  /**
   * Allocates a dynamic capacity mapped buffer in {@link java.nio.channels.FileChannel.MapMode#READ_WRITE} mode with an initial capacity
   * of {@code 16MiB} and a maximum capacity of {@link Integer#MAX_VALUE}.
   * <p>
   * The resulting buffer will have a maximum capacity of {@link Integer#MAX_VALUE}. As bytes are written to the buffer
   * its capacity will double in count each time the current capacity is reached. Memory will be mapped by opening and
   * expanding the given {@link java.io.File} to the desired {@code capacity} and mapping the file contents into memory via
   * {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   *
   * @param file The file to map into memory.
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   *
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode)
   * @see #allocate(java.io.File, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long)
   * @see #allocate(java.io.File, long, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long, long)
   */
  public static MappedBuffer allocate(File file) {
    return allocate(file, FileChannel.MapMode.READ_WRITE, DEFAULT_INITIAL_CAPACITY, Long.MAX_VALUE);
  }

  /**
   * Allocates a dynamic capacity mapped buffer in {@link java.nio.channels.FileChannel.MapMode#READ_WRITE} mode with an initial capacity
   * of {@code 16MiB} and a maximum capacity of {@link Integer#MAX_VALUE}.
   * <p>
   * The resulting buffer will be initialized to a capacity of {@code 4096} and have a maximum capacity of
   * {@link Integer#MAX_VALUE}. As bytes are written to the buffer its capacity will double in count each time the current
   * capacity is reached. Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired
   * {@code capacity} and mapping the file contents into memory via
   * {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   *
   * @param file The file to map into memory.
   * @param mode The mode with which to map the file.
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   *
   * @see #allocate(java.io.File)
   * @see #allocate(java.io.File, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long)
   * @see #allocate(java.io.File, long, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long, long)
   */
  public static MappedBuffer allocate(File file, FileChannel.MapMode mode) {
    return allocate(file, mode, DEFAULT_INITIAL_CAPACITY, Long.MAX_VALUE);
  }

  /**
   * Allocates a fixed capacity mapped buffer in {@link java.nio.channels.FileChannel.MapMode#READ_WRITE} mode.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired {@code capacity} and mapping the
   * file contents into memory via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   * <p>
   * The resulting buffer will have a capacity of {@code capacity}. The underlying {@link UnsafeMappedBytes} will be
   * initialized to the next power of {@code 2}.
   *
   * @param file The file to map into memory.
   * @param capacity The fixed capacity of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   * @throws IllegalArgumentException If the {@code capacity} is greater than {@link Integer#MAX_VALUE}.
   *
   * @see #allocate(java.io.File)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long)
   * @see #allocate(java.io.File, long, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long, long)
   */
  public static MappedBuffer allocate(File file, long capacity) {
    return allocate(file, FileChannel.MapMode.READ_WRITE, capacity, capacity);
  }

  /**
   * Allocates a fixed capacity mapped buffer in the given {@link java.nio.channels.FileChannel.MapMode}.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired {@code capacity} and mapping the
   * file contents into memory via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   * <p>
   * The resulting buffer will have a capacity of {@code capacity}. The underlying {@link UnsafeMappedBytes} will be
   * initialized to the next power of {@code 2}.
   *
   * @param file The file to map into memory.
   * @param mode The mode with which to map the file.
   * @param capacity The fixed capacity of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   * @throws IllegalArgumentException If the {@code capacity} is greater than {@link Integer#MAX_VALUE}.
   *
   * @see #allocate(java.io.File)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode)
   * @see #allocate(java.io.File, long)
   * @see #allocate(java.io.File, long, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long, long)
   */
  public static MappedBuffer allocate(File file, FileChannel.MapMode mode, long capacity) {
    return allocate(file, mode, capacity, capacity);
  }

  /**
   * Allocates a mapped buffer.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired {@code count} and mapping the
   * file contents into memory via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   * <p>
   * The resulting buffer will have a capacity of {@code initialCapacity}. The underlying {@link UnsafeMappedBytes} will be
   * initialized to the next power of {@code 2}. As bytes are written to the buffer, the buffer's capacity will double
   * as long as {@code maxCapacity > capacity}.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param initialCapacity The initial capacity of the buffer.
   * @param maxCapacity The maximum capacity of the buffer.
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   * @throws IllegalArgumentException If the {@code capacity} or {@code maxCapacity} is greater than
   *         {@link Integer#MAX_VALUE}.
   *
   * @see #allocate(java.io.File)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode)
   * @see #allocate(java.io.File, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long, long)
   */
  public static MappedBuffer allocate(File file, long initialCapacity, long maxCapacity) {
    return allocate(file, FileChannel.MapMode.READ_WRITE, initialCapacity, maxCapacity);
  }

  /**
   * Allocates a mapped buffer.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired {@code count} and mapping the
   * file contents into memory via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   * <p>
   * The resulting buffer will have a capacity of {@code initialCapacity}. The underlying {@link UnsafeMappedBytes} will be
   * initialized to the next power of {@code 2}. As bytes are written to the buffer, the buffer's capacity will double
   * as long as {@code maxCapacity > capacity}.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param mode The mode with which to map the file.
   * @param initialCapacity The initial capacity of the buffer.
   * @param maxCapacity The maximum capacity of the buffer.
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   * @throws IllegalArgumentException If the {@code capacity} or {@code maxCapacity} is greater than
   *         {@link Integer#MAX_VALUE}.
   *
   * @see #allocate(java.io.File)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode)
   * @see #allocate(java.io.File, long)
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long)
   * @see #allocate(java.io.File, long, long)
   */
  public static MappedBuffer allocate(File file, FileChannel.MapMode mode, long initialCapacity, long maxCapacity) {
    if (file == null)
      throw new NullPointerException("file cannot be null");
    if (mode == null)
      mode = FileChannel.MapMode.READ_WRITE;
    if (initialCapacity > maxCapacity)
      throw new IllegalArgumentException("initial capacity cannot be greater than maximum capacity");
    if (initialCapacity > Integer.MAX_VALUE)
      throw new IllegalArgumentException("initial capacity for MappedBuffer cannot be greater than " + Integer.MAX_VALUE);
    if (maxCapacity > Integer.MAX_VALUE)
      throw new IllegalArgumentException("maximum capacity for MappedBuffer cannot be greater than " + Integer.MAX_VALUE);
    return new MappedBuffer(MappedBytes.allocate(file, initialCapacity), 0, initialCapacity, maxCapacity);
  }

  protected MappedBuffer(MappedBytes bytes, long offset, long initialCapacity, long maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity, null);
  }

  /**
   * Deletes the underlying file.
   */
  public void delete() {
    ((MappedBytes) bytes).delete();
  }

}
