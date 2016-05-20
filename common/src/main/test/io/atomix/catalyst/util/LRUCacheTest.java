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
package io.atomix.catalyst.util;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

@Test
public class LRUCacheTest {

  public void testLRUBehavior() {
    LRUCache<String, String> cache = new LRUCache<>(2);
    cache.put("key1", "v1");
    cache.put("key2", "v2");

    // key3 should replace key1
    cache.put("key3", "v3");
    assertFalse(cache.containsKey("key1"));

    // this marks key2 as most recently used
    assertEquals("v2", cache.get("key2"));

    // key4 should replace key3
    cache.put("key4", "v4");
    assertFalse(cache.containsKey("key3"));
  }
}