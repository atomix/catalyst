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
 * limitations under the License
 */
package io.atomix.catalyst.util.hash;

/**
 * Hash function.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Hasher {

  /**
   * Returns a 32-bit hash of the given string.
   *
   * @param value The string to hash.
   * @return The 32-bit hash of the given string.
   */
  default int hash32(String value) {
    return hash32(value.getBytes());
  }

  /**
   * Returns the 32-bit hash of the given bytes.
   *
   * @param bytes The bytes to hash.
   * @return The 32-bit hash of the given bytes.
   */
  int hash32(byte[] bytes);

  /**
   * Returns a 64-bit hash of the given string.
   *
   * @param value The string to hash.
   * @return The 64-bit hash of the given string.
   */
  default long hash64(String value) {
    return hash64(value.getBytes());
  }

  /**
   * Returns the 64-bit hash of the given bytes.
   *
   * @param bytes The bytes to hash.
   * @return The 64-bit hash of the given bytes.
   */
  long hash64(byte[] bytes);

}
