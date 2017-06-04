package io.atomix.catalyst.buffer;

import io.atomix.catalyst.util.reference.ReferenceManager;

/**
 * {@link java.nio.ByteBuffer} based buffer.
 */
public abstract class ByteBufferBuffer extends AbstractBuffer {
  protected final ByteBufferBytes bytes;

  public ByteBufferBuffer(ByteBufferBytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
    this.bytes = bytes;
  }

  public ByteBufferBuffer(ByteBufferBytes bytes, long offset, long initialCapacity, long maxCapacity, ReferenceManager<Buffer> referenceManager) {
    super(bytes, offset, initialCapacity, maxCapacity, referenceManager);
    this.bytes = bytes;
  }

  @Override
  public byte[] array() {
    return bytes.array();
  }

  @Override
  protected void compact(long from, long to, long length) {
    byte[] bytes = new byte[1024];
    long position = from;
    while (position < from + length) {
      long size = Math.min((from + length) - position, 1024);
      this.bytes.read(position, bytes, 0, size);
      this.bytes.write(0, bytes, 0, size);
      position += size;
    }
  }

}
