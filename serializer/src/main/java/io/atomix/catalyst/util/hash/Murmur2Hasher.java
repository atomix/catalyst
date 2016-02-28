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
 * Murmur3 hasher.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Murmur2Hasher implements Hasher {
  private final int seed;

  public Murmur2Hasher() {
    this.seed = 0x9747b28c;
  }

  public Murmur2Hasher(int seed) {
    this.seed = seed;
  }

  @Override
  public int hash32(byte[] bytes) {
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    final int m = 0x5bd1e995;
    final int r = 24;

    int length = bytes.length;
    int length4 = bytes.length / 4;

    // Initialize the hash to a random value
    int h = seed ^ length;

    for (int i = 0; i < length4; i++) {
      final int i4 = i * 4;
      int k = (bytes[i4] & 0xff) + ((bytes[i4 + 1] & 0xff) << 8)
        + ((bytes[i4 + 2] & 0xff) << 16) + ((bytes[i4 + 3] & 0xff) << 24);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // Handle the last few bytes of the input array
    switch (length % 4) {
      case 3:
        h ^= (bytes[(length & ~3) + 2] & 0xff) << 16;
      case 2:
        h ^= (bytes[(length & ~3) + 1] & 0xff) << 8;
      case 1:
        h ^= (bytes[length & ~3] & 0xff);
        h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }

  @Override
  public long hash64(byte[] bytes) {
    final long m = 0xc6a4a7935bd1e995L;
    final int r = 47;

    int length = bytes.length;
    int length8 = bytes.length / 8;

    long h = (seed & 0xffffffffl) ^ (length * m);

    for (int i = 0; i < length8; i++) {
      final int i8 = i * 8;
      long k = ((long) bytes[i8] & 0xff) + (((long) bytes[i8 + 1] & 0xff) << 8)
        + (((long) bytes[i8 + 2] & 0xff) << 16) + (((long) bytes[i8 + 3] & 0xff) << 24)
        + (((long) bytes[i8 + 4] & 0xff) << 32) + (((long) bytes[i8 + 5] & 0xff) << 40)
        + (((long) bytes[i8 + 6] & 0xff) << 48) + (((long) bytes[i8 + 7] & 0xff) << 56);

      k *= m;
      k ^= k >>> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    switch (length % 8) {
      case 7:
        h ^= (long) (bytes[(length & ~7) + 6] & 0xff) << 48;
      case 6:
        h ^= (long) (bytes[(length & ~7) + 5] & 0xff) << 40;
      case 5:
        h ^= (long) (bytes[(length & ~7) + 4] & 0xff) << 32;
      case 4:
        h ^= (long) (bytes[(length & ~7) + 3] & 0xff) << 24;
      case 3:
        h ^= (long) (bytes[(length & ~7) + 2] & 0xff) << 16;
      case 2:
        h ^= (long) (bytes[(length & ~7) + 1] & 0xff) << 8;
      case 1:
        h ^= (long) (bytes[length & ~7] & 0xff);
        h *= m;
    }

    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;

    return h;
  }

}
