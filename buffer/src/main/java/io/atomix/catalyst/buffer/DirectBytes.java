package io.atomix.catalyst.buffer;

import io.atomix.catalyst.buffer.util.HeapMemory;

import java.nio.ByteBuffer;

/**
 * {@link ByteBuffer} based direct bytes.
 */
public class DirectBytes extends ByteBufferBytes {

  /**
   * Allocates a new direct byte array.
   *
   * @param size The count of the buffer to allocate (in bytes).
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
   *         an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   */
  public static DirectBytes allocate(long size) {
    if (size > HeapMemory.MAX_SIZE)
      throw new IllegalArgumentException("size cannot for DirectBytes cannot be greater than " + HeapMemory.MAX_SIZE);
    return new DirectBytes(ByteBuffer.allocate((int) size));
  }

  protected DirectBytes(ByteBuffer buffer) {
    super(buffer);
  }

  @Override
  protected ByteBuffer newByteBuffer(long size) {
    return ByteBuffer.allocateDirect((int) size);
  }

  @Override
  public boolean isDirect() {
    return true;
  }

}
