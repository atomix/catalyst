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
package io.atomix.catalyst.serializer;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;

/**
 * Provides an interface for serializable types.
 * <p>
 * Classes can implement this interface as an alternative to providing a separate {@link TypeSerializer} instance. Note,
 * however, that {@link CatalystSerializable} classes must still be registered via {@link Serializer#register(Class)}.
 * <p>
 * Types that implement this interface should provide a no-argument constructor via which Catalyst can allocate new
 * instances of the class. During serialization, Catalyst will call {@link CatalystSerializable#writeObject(BufferOutput, Serializer)}
 * to serialize the object to a {@link Buffer}. During deserialization, Catalyst will call
 * {@link CatalystSerializable#readObject(BufferInput, Serializer)} to deserialize
 * the object from a {@link Buffer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CatalystSerializable {

  /**
   * Writes the object to the given buffer.
   * <p>
   * Implementations of this method should write object attributes to the given buffer. Note that assumptions cannot be
   * safely made about the given buffer's {@link Buffer#position()}, {@link Buffer#limit()},
   * or any other navigational attributes of the provided {@link Buffer}. To navigate the buffer,
   * set the buffer's {@link Buffer#mark()} and {@link Buffer#reset()}.
   * <p>
   * When writing dynamically sized attributes such as strings and collections, users should always write the attribute's
   * count to the given buffer. Catalyst makes no guarantee that the buffer provided to
   * {@link CatalystSerializable#readObject(BufferInput, Serializer)} will reflect the
   * number of bytes written to the buffer during serialization.
   *
   * @param buffer The buffer to which to write the object.
   * @param serializer The serializer with which the object is being serialized.
   * @see CatalystSerializable#readObject(BufferInput, Serializer)
   */
  void writeObject(BufferOutput buffer, Serializer serializer);

  /**
   * Reads the object from the given buffer.
   * <p>
   * Implementations of this method should read object attributes from the given buffer in the same order with which they
   * were written to the buffer in {@link CatalystSerializable#writeObject(BufferOutput, Serializer)}.
   * Catalyst guarantees only that the current {@link Buffer#position()} will reflect the start
   * of the bytes written by {@link CatalystSerializable#writeObject(BufferOutput, Serializer)},
   * but not that the {@link Buffer#remaining()} bytes reflect the number of bytes written by
   * {@link CatalystSerializable#writeObject(BufferOutput, Serializer)}.
   *
   * @param buffer The buffer from which to read the object.
   * @param serializer The serializer with which the object is being serialized.
   * @see CatalystSerializable#writeObject(BufferOutput, Serializer)
   */
  void readObject(BufferInput buffer, Serializer serializer);

}
