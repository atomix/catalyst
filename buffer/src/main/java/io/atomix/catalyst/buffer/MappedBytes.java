package io.atomix.catalyst.buffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

/**
 * {@link ByteBuffer} based mapped bytes.
 */
public class MappedBytes extends ByteBufferBytes {

  /**
   * Allocates a mapped buffer in {@link java.nio.channels.FileChannel.MapMode#READ_WRITE} mode.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired {@code count} and mapping the
   * file contents into memory via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param size The count of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   * @throws IllegalArgumentException If {@code count} is greater than {@link Integer#MAX_VALUE}
   *
   * @see #allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long)
   */
  public static MappedBytes allocate(File file, long size) {
    return allocate(file, FileChannel.MapMode.READ_WRITE, size);
  }

  /**
   * Allocates a mapped buffer.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired {@code count} and mapping the
   * file contents into memory via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param mode The mode with which to map the file.
   * @param size The count of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   * @throws IllegalArgumentException If {@code count} is greater than {@link Integer#MAX_VALUE}
   *
   * @see #allocate(java.io.File, long)
   */
  public static MappedBytes allocate(File file, FileChannel.MapMode mode, long size) {
    if (file == null)
      throw new NullPointerException("file cannot be null");
    if (mode == null)
      mode = FileChannel.MapMode.READ_WRITE;
    if (size > Integer.MAX_VALUE)
      throw new IllegalArgumentException("size for MappedBytes cannot be greater than " + Integer.MAX_VALUE);

    RandomAccessFile randomAccessFile = createFile(file, mode);
    try {
      MappedByteBuffer buffer = randomAccessFile.getChannel().map(mode, 0, size);
      return new MappedBytes(file, randomAccessFile, buffer, mode);
    } catch (IOException e) {
      throw new CatalystIOException(e);
    }
  }

  private final File file;
  private final RandomAccessFile randomAccessFile;
  private final FileChannel.MapMode mode;

  protected MappedBytes(File file, RandomAccessFile randomAccessFile, MappedByteBuffer buffer, FileChannel.MapMode mode) {
    super(buffer);
    this.file = file;
    this.randomAccessFile = randomAccessFile;
    this.mode = mode;
  }

  @Override
  protected ByteBuffer newByteBuffer(long size) {
    try {
      return randomAccessFile.getChannel().map(mode, 0, size);
    } catch (IOException e) {
      throw new CatalystIOException(e);
    }
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public Bytes flush() {
    ((MappedByteBuffer) buffer).force();
    return this;
  }

  @Override
  public void close() {
    try {
      randomAccessFile.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    super.close();
  }

  /**
   * Deletes the underlying file.
   */
  public void delete() {
    try {
      close();
      Files.delete(file.toPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static RandomAccessFile createFile(File file, FileChannel.MapMode mode) {
    if (file == null)
      throw new NullPointerException("file cannot be null");
    if (mode == null)
      mode = FileChannel.MapMode.READ_WRITE;
    try {
      return new RandomAccessFile(file, parseMode(mode));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String parseMode(FileChannel.MapMode mode) {
    if (mode == FileChannel.MapMode.READ_ONLY) {
      return "r";
    } else if (mode == FileChannel.MapMode.READ_WRITE) {
      return "rw";
    }
    throw new IllegalArgumentException("unsupported map mode");
  }

}
