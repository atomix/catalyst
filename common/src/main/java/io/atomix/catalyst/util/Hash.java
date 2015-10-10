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
package io.atomix.catalyst.util;

/**
 * Hash code utilities.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Hash {

  private Hash() {
  }

  /**
   * Returns a hash for the given string.
   *
   * @param value The string to hash.
   * @return The hash value.
   */
  public static int hash(String value) {
    return hash(value.toCharArray());
  }

  /**
   * Returns a hash for the given character array.
   *
   * @param value The character array to hash.
   * @return The hash value.
   */
  public static int hash(char[] value) {
    int hashCode = 0;
    for (int i = 0; i < value.length; i++) {
      hashCode = 31 * hashCode + value[i];
    }
    return hashCode;
  }

}
