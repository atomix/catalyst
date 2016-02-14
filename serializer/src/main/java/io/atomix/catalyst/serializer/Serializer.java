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

import io.atomix.catalyst.buffer.*;
import io.atomix.catalyst.serializer.util.PooledTypeSerializer;
import io.atomix.catalyst.util.ReferenceCounted;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Catalyst serializer.
 * <p>
 * This class provides an interface for efficient serialization of Java objects. Serialization is performed by
 * {@link TypeSerializer} instances. Objects that can be serialized by {@link Serializer} must be registered. When objects
 * are serialized, Catalyst will write the object's type as an 16-bit unsigned integer. When reading objects, the
 * 16-bit identifier is used to construct a new object.
 * <p>
 * Serializable objects must either provide a {@link TypeSerializer}. implement {@link CatalystSerializable}, or implement
 * {@link java.io.Externalizable}. For efficiency, serializable objects may implement {@link ReferenceCounted}
 * or provide a {@link PooledTypeSerializer} that reuses objects during deserialization.
 * Catalyst will automatically deserialize {@link ReferenceCounted} types using an object pool.
 * <p>
 * Serialization via this class is not thread safe.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Serializer {
  private final SerializerRegistry registry;
  private final Map<Class<?>, TypeSerializer<?>> serializers = new HashMap<>();
  private final Map<Class<?>, Integer> ids = new HashMap<>();
  private final Map<String, Class<?>> types = new HashMap<>();
  private final BufferAllocator allocator;
  private final AtomicBoolean whitelistRequired;

  /**
   * Creates a new serializer instance with a default {@link UnpooledHeapAllocator}.
   * <p>
   * Catalyst will use a {@link UnpooledHeapAllocator} to allocate buffers for serialization.
   * Users can explicitly allocate buffers with the heap allocator via {@link Serializer#allocate(long)}.
   * <p>
   * <pre>
   *   {@code
   *      Serializer serializer = new Serializer(new PooledHeapAllocator());
   *      Buffer buffer = serializer.allocate(1024);
   *   }
   * </pre>
   */
  public Serializer() {
    this(new UnpooledHeapAllocator(), new SerializableTypeResolver[0]);
  }

  /**
   * Creates a new serializer instance with a buffer allocator.
   * <p>
   * The given {@link BufferAllocator} will be used to allocate buffers during serialization.
   * Users can explicitly allocate buffers with the given allocator via {@link Serializer#allocate(long)}.
   * <p>
   * <pre>
   *   {@code
   *      Serializer serializer = new Serializer(new PooledHeapAllocator());
   *      Buffer buffer = serializer.allocate(1024);
   *   }
   * </pre>
   * <p>
   * If a {@link PooledAllocator} is used, users must be careful to release buffers back to the
   * pool by calling {@link Buffer#release()} or {@link Buffer#close()}.
   *
   * @param allocator The serializer buffer allocator.
   */
  public Serializer(BufferAllocator allocator) {
    this(allocator, new SerializableTypeResolver[0]);
  }

  /**
   * Creates a new serializer instance with a default {@link UnpooledHeapAllocator}.
   * <p>
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link PrimitiveTypeResolver} and {@link JdkTypeResolver}
   * will be used to register common serializable types, and any additional types will be registered via provided type
   * resolvers thereafter.
   *
   * @param resolvers A collection of serializable type resolvers with which to register serializable types.
   */
  public Serializer(SerializableTypeResolver... resolvers) {
    this(new UnpooledHeapAllocator(), resolvers);
  }

  /**
   * Creates a new serializer instance with a default {@link UnpooledHeapAllocator}.
   * <p>
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link PrimitiveTypeResolver} and {@link JdkTypeResolver}
   * will be used to register common serializable types, and any additional types will be registered via provided type
   * resolvers thereafter.
   *
   * @param resolvers A collection of serializable type resolvers with which to register serializable types.
   */
  public Serializer(Collection<SerializableTypeResolver> resolvers) {
    this(new UnpooledHeapAllocator(), resolvers);
  }

  /**
   * Creates a new serializer instance with a buffer allocator and type resolver(s).
   * <p>
   * The given {@link BufferAllocator} will be used to allocate buffers during serialization.
   * Users can explicitly allocate buffers with the given allocator via {@link Serializer#allocate(long)}.
   * <p>
   * <pre>
   *   {@code
   *      Serializer serializer = new Serializer(new PooledHeapAllocator());
   *      Buffer buffer = serializer.allocate(1024);
   *   }
   * </pre>
   * <p>
   * If a {@link PooledAllocator} is used, users must be careful to release buffers back to the
   * pool by calling {@link Buffer#release()} or {@link Buffer#close()}.
   * <p>
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link PrimitiveTypeResolver} and {@link JdkTypeResolver}
   * will be used to register common serializable types, and any additional types will be registered via provided type
   * resolvers thereafter.
   *
   * @param allocator The serializer buffer allocator.
   * @param resolvers A collection of serializable type resolvers with which to register serializable types.
   */
  @SuppressWarnings("unchecked")
  public Serializer(BufferAllocator allocator, SerializableTypeResolver... resolvers) {
    this(allocator, resolvers != null ? Arrays.asList(resolvers) : Collections.EMPTY_LIST);
  }

  /**
   * Creates a new serializer instance with a buffer allocator and type resolver(s).
   * <p>
   * The given {@link BufferAllocator} will be used to allocate buffers during serialization.
   * Users can explicitly allocate buffers with the given allocator via {@link Serializer#allocate(long)}.
   * <p>
   * <pre>
   *   {@code
   *      Serializer serializer = new Serializer(new PooledHeapAllocator());
   *      Buffer buffer = serializer.allocate(1024);
   *   }
   * </pre>
   * <p>
   * If a {@link PooledAllocator} is used, users must be careful to release buffers back to the
   * pool by calling {@link Buffer#release()} or {@link Buffer#close()}.
   * <p>
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link PrimitiveTypeResolver} and {@link JdkTypeResolver}
   * will be used to register common serializable types, and any additional types will be registered via provided type
   * resolvers thereafter.
   *
   * @param allocator The serializer buffer allocator.
   * @param resolvers A collection of serializable type resolvers with which to register serializable types.
   */
  public Serializer(BufferAllocator allocator, Collection<SerializableTypeResolver> resolvers) {
    if (allocator == null)
      throw new NullPointerException("allocator cannot be null");
    this.allocator = allocator;
    this.whitelistRequired = new AtomicBoolean(true);
    registry = new SerializerRegistry(resolvers);
  }

  private Serializer(SerializerRegistry registry, BufferAllocator allocator, AtomicBoolean whitelistRequired) {
    this.registry = registry;
    this.allocator = allocator;
    this.whitelistRequired = whitelistRequired;
  }

  /**
   * Enables whitelisting for serializable types.
   * <p>
   * When whitelisting is enabled, only types that are registered with the {@link SerializerRegistry}
   * can be serialized and deserialized, and classes will never be loaded by class names. This prevents
   * certain types of attacks in untrusted networks.
   *
   * @return The serializer.
   */
  public Serializer enableWhitelist() {
    whitelistRequired.set(true);
    return this;
  }

  /**
   * Disables whitelisting for serializable types.
   * <p>
   * When whitelisting is disabled, types that are not registered may be serialized and deserialized
   * by this serializer. This can pose a security risk in an untrusted network. It's recommended that
   * users enable whitelisting and register serializable classes.
   *
   * @return The serializer.
   */
  public Serializer disableWhitelist() {
    whitelistRequired.set(false);
    return this;
  }

  /**
   * Sets whether whitelisting of class names is required.
   *
   * @param whitelistRequired Indicates whether to require whitelisting of class names.
   * @return The serializer.
   */
  public Serializer withWhitelistRequired(boolean whitelistRequired) {
    this.whitelistRequired.set(whitelistRequired);
    return this;
  }

  /**
   * Indicates whether whitelisting is enabled for the serializer.
   *
   * @return Whether whitelisting is enabled for the serializer.
   */
  public boolean isWhitelistRequired() {
    return whitelistRequired.get();
  }

  /**
   * Returns the serializer registry.
   *
   * @return The underlying serializable type registry.
   */
  public SerializerRegistry registry() {
    return registry;
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to an existing {@link Serializer} instance. Types resolved
   * by the provided resolver(s) will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types.
   * @return The serializer instance.
   */
  public Serializer resolve(SerializableTypeResolver... resolvers) {
    registry.resolve(resolvers);
    return this;
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to an existing {@link Serializer} instance. Types resolved
   * by the provided resolver(s) will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types.
   * @return The serializer instance.
   */
  public Serializer resolve(Collection<SerializableTypeResolver> resolvers) {
    registry.resolve(resolvers);
    return this;
  }

  /**
   * Registers a serializable type.
   * <p>
   * The serializable type must be assignable from {@link CatalystSerializable} or
   * {@link java.io.Externalizable}.
   *
   * @param type The serializable type. This type must be assignable from {@link CatalystSerializable}
   *             or {@link java.io.Externalizable}.
   * @return The serializer instance.
   * @throws java.lang.IllegalArgumentException If the serializable type ID is within the reserved range `128` to `255`
   */
  public Serializer register(Class<?> type) {
    registry.register(type);
    return this;
  }

  /**
   * Registers a serializable type with an identifier.
   * <p>
   * The serializable type must be assignable from {@link CatalystSerializable} or
   * {@link java.io.Externalizable}.
   *
   * @param type The serializable type. This type must be assignable from {@link CatalystSerializable}
   *             or {@link java.io.Externalizable}.
   * @param id The type ID. This ID must be a number between `0` and `65535`. Serialization IDs between `128` and `255`
   *           are reserved and will result in an {@link java.lang.IllegalArgumentException}
   * @return The serializer instance.
   * @throws java.lang.IllegalArgumentException If the serializable type ID is within the reserved range `128` to `255`
   */
  public Serializer register(Class<?> type, int id) {
    registry.register(type, id);
    return this;
  }

  /**
   * Registers a type serializer.
   * <p>
   * Because a custom {@link TypeSerializer} is provided, the registered {@code type} can be any class and does not have to
   * implement any particular interface.
   * <p>
   * Internally, the provided class will be wrapped in a {@link DefaultTypeSerializerFactory}. The serializer
   * class can be registered for more than one {@code type} class. The factory will instantiate a new
   * {@link TypeSerializer} instance once for each type for which the serializer is registered per {@link Serializer}
   * instance. If the {@code Serializer} instance is {@link Serializer#clone() cloned}, the serializer
   * factory will be copied and a new {@link TypeSerializer} will be instantiated for the clone.
   *
   * @param type The serializable type.
   * @param serializer The serializer to register.
   * @return The serializer instance.
   */
  public Serializer register(Class<?> type, Class<? extends TypeSerializer<?>> serializer) {
    registry.register(type, serializer);
    return this;
  }

  /**
   * Registers a type serializer factory.
   * <p>
   * Because a custom {@link TypeSerializerFactory} is provided, the registered {@code type} can be any class and does not have to
   * implement any particular interface.
   * <p>
   * The serializer factory can be registered for more than one {@code type} class. The factory will be called on to
   * create a new {@link TypeSerializer} instance once for each type for which the serializer is
   * registered per {@link Serializer} instance. If the {@code Serializer} instance is {@link Serializer#clone() cloned},
   * the serializer factory will be copied and a new {@link TypeSerializer} will be instantiated for the clone.
   *
   * @param type The serializable type.
   * @param factory The serializer factory to register.
   * @return The serializer instance.
   */
  public Serializer register(Class<?> type, TypeSerializerFactory factory) {
    registry.register(type, factory);
    return this;
  }

  /**
   * Registers a type serializer with an identifier.
   * <p>
   * The provided serializable type ID will be used to identify the serializable type during serialization and deserialization.
   * When objects of the given {@code type} are serialized to a {@link Buffer}, the given type
   * {@code id} will be written to the buffer in lieu of its class name. When the object is deserialized, the type {@code id}
   * will be used to look up the class. It is essential that the given {@code type} be registered with the same {@code id}
   * on all {@link Serializer} instances.
   * <p>
   * Because a custom {@link TypeSerializer} is provided, the registered {@code type} can be any class and does not have to
   * implement any particular interface.
   * <p>
   * Internally, the provided class will be wrapped in a {@link DefaultTypeSerializerFactory}. The serializer
   * class can be registered for more than one {@code type} class. The factory will instantiate a new
   * {@link TypeSerializer} instance once for each type for which the serializer is registered per {@link Serializer}
   * instance. If the {@code Serializer} instance is {@link Serializer#clone() cloned}, the serializer
   * factory will be copied and a new {@link TypeSerializer} will be instantiated for the clone.
   *
   * @param type The serializable type.
   * @param serializer The serializer to register.
   * @param id The type ID.
   * @return The serializer instance.
   */
  public Serializer register(Class<?> type, Class<? extends TypeSerializer<?>> serializer, int id) {
    registry.register(type, serializer, id);
    return this;
  }

  /**
   * Registers a type serializer with an identifier.
   * <p>
   * The provided serializable type ID will be used to identify the serializable type during serialization and deserialization.
   * When objects of the given {@code type} are serialized to a {@link Buffer}, the given type
   * {@code id} will be written to the buffer in lieu of its class name. When the object is deserialized, the type {@code id}
   * will be used to look up the class. It is essential that the given {@code type} be registered with the same {@code id}
   * on all {@link Serializer} instances.
   * <p>
   * Because a custom {@link TypeSerializerFactory} is provided, the registered {@code type} can be any class and does not have to
   * implement any particular interface.
   * <p>
   * The serializer factory can be registered for more than one {@code type} class. The factory will be called on to
   * create a new {@link TypeSerializer} instance once for each type for which the serializer is
   * registered per {@link Serializer} instance. If the {@code Serializer} instance is {@link Serializer#clone() cloned},
   * the serializer factory will be copied and a new {@link TypeSerializer} will be instantiated for the clone.
   *
   * @param type The serializable type.
   * @param factory The serializer factory to register.
   * @param id The type ID.
   * @return The serializer instance.
   */
  public Serializer register(Class<?> type, TypeSerializerFactory factory, int id) {
    registry.register(type, factory, id);
    return this;
  }

  /**
   * Registers an abstract type serializer for the given abstract type.
   * <p>
   * Abstract serializers allow abstract types to be serialized without explicitly registering a concrete type.
   * The concept of abstract serializers differs from {@link #registerDefault(Class, TypeSerializerFactory) default serializers}
   * in that abstract serializers can be registered with a serializable type ID, and types {@link #register(Class) registered}
   * without a specific {@link TypeSerializer} do not inheret from abstract serializers.
   * <pre>
   *   {@code
   *   serializer.registerAbstract(List.class, AbstractListSerializer.class);
   *   }
   * </pre>
   *
   * @param abstractType The abstract type for which to register the abstract serializer. Types that extend
   *                     the abstract type will be serialized using the given abstract serializer unless a
   *                     serializer has been registered for the specific concrete type.
   * @param serializer The abstract type serializer with which to serialize instances of the abstract type.
   * @return The serializer.
   * @throws NullPointerException if the {@code abstractType} or {@code serializer} is {@code null}
   */
  public Serializer registerAbstract(Class<?> abstractType, Class<? extends TypeSerializer<?>> serializer) {
    registry.registerAbstract(abstractType, serializer);
    return this;
  }

  /**
   * Registers an abstract type serializer for the given abstract type.
   * <p>
   * Abstract serializers allow abstract types to be serialized without explicitly registering a concrete type.
   * The concept of abstract serializers differs from {@link #registerDefault(Class, TypeSerializerFactory) default serializers}
   * in that abstract serializers can be registered with a serializable type ID, and types {@link #register(Class) registered}
   * without a specific {@link TypeSerializer} do not inheret from abstract serializers.
   * <pre>
   *   {@code
   *   serializer.registerAbstract(List.class, AbstractListSerializer.class);
   *   }
   * </pre>
   *
   * @param abstractType The abstract type for which to register the abstract serializer. Types that extend
   *                     the abstract type will be serialized using the given abstract serializer unless a
   *                     serializer has been registered for the specific concrete type.
   * @param factory The abstract type serializer factory with which to serialize instances of the abstract type.
   * @return The serializer.
   * @throws NullPointerException if the {@code abstractType} or {@code serializer} is {@code null}
   */
  public Serializer registerAbstract(Class<?> abstractType, TypeSerializerFactory factory) {
    registry.registerAbstract(abstractType, factory);
    return this;
  }

  /**
   * Registers an abstract type serializer for the given abstract type.
   * <p>
   * Abstract serializers allow abstract types to be serialized without explicitly registering a concrete type.
   * The concept of abstract serializers differs from {@link #registerDefault(Class, TypeSerializerFactory) default serializers}
   * in that abstract serializers can be registered with a serializable type ID, and types {@link #register(Class) registered}
   * without a specific {@link TypeSerializer} do not inheret from abstract serializers.
   * <pre>
   *   {@code
   *   serializer.registerAbstract(List.class, AbstractListSerializer.class);
   *   }
   * </pre>
   *
   * @param abstractType The abstract type for which to register the abstract serializer. Types that extend
   *                     the abstract type will be serialized using the given abstract serializer unless a
   *                     serializer has been registered for the specific concrete type.
   * @param serializer The abstract type serializer with which to serialize instances of the abstract type.
   * @param id The serializable type ID with which to serialize the abstract class.
   * @return The serializer.
   * @throws NullPointerException if the {@code abstractType} or {@code serializer} is {@code null}
   */
  public Serializer registerAbstract(Class<?> abstractType, Class<? extends TypeSerializer<?>> serializer, int id) {
    registry.registerAbstract(abstractType, serializer, id);
    return this;
  }

  /**
   * Registers an abstract type serializer for the given abstract type.
   * <p>
   * Abstract serializers allow abstract types to be serialized without explicitly registering a concrete type.
   * The concept of abstract serializers differs from {@link #registerDefault(Class, TypeSerializerFactory) default serializers}
   * in that abstract serializers can be registered with a serializable type ID, and types {@link #register(Class) registered}
   * without a specific {@link TypeSerializer} do not inheret from abstract serializers.
   * <pre>
   *   {@code
   *   serializer.registerAbstract(List.class, AbstractListSerializer.class);
   *   }
   * </pre>
   *
   * @param abstractType The abstract type for which to register the abstract serializer. Types that extend
   *                     the abstract type will be serialized using the given abstract serializer unless a
   *                     serializer has been registered for the specific concrete type.
   * @param factory The abstract type serializer factory with which to serialize instances of the abstract type.
   * @param id The serializable type ID with which to serialize the abstract class.
   * @return The serializer.
   * @throws NullPointerException if the {@code abstractType} or {@code serializer} is {@code null}
   */
  public Serializer registerAbstract(Class<?> abstractType, TypeSerializerFactory factory, int id) {
    registry.registerAbstract(abstractType, factory, id);
    return this;
  }

  /**
   * Registers a default type serializer for the given base type.
   * <p>
   * Default serializers are used to serialize types for which no specific {@link TypeSerializer} is provided.
   * When a serializable type is {@link #register(Class) registered} without a {@link TypeSerializer}, the
   * first default serializer found for the given type is assigned as the serializer for that type. Default
   * serializers are evaluated against registered types in reverse insertion order, so default serializers
   * registered more recently take precedence over default serializers registered earlier.
   * <pre>
   *   {@code
   *   serializer.registerDefault(Serializable.class, SerializableSerializer.class);
   *   serializer.register(SomeSerializable.class, 1);
   *   }
   * </pre>
   * If an object of a type that has not been {@link #register(Class) registered} is
   * {@link #writeObject(Object) serialized} and {@link #isWhitelistRequired() whitelisting} is disabled,
   * the object will be serialized with the class name and a default serializer if one is found.
   *
   * @param baseType The base type for which to register the default serializer. Types that extend the base
   *                 type and are registered without a specific {@link TypeSerializer} will be serialized
   *                 using the registered default {@link TypeSerializer}.
   * @param serializer The default type serializer with which to serialize instances of the base type.
   * @return The serializer.
   * @throws NullPointerException if either argument is {@code null}
   */
  public Serializer registerDefault(Class<?> baseType, Class<? extends TypeSerializer> serializer) {
    registry.registerDefault(baseType, serializer);
    return this;
  }

  /**
   * Registers a default type serializer for the given base type.
   * <p>
   * Default serializers are used to serialize types for which no specific {@link TypeSerializer} is provided.
   * When a serializable type is {@link #register(Class) registered} without a {@link TypeSerializer}, the
   * first default serializer found for the given type is assigned as the serializer for that type. Default
   * serializers are evaluated against registered types in reverse insertion order, so default serializers
   * registered more recently take precedence over default serializers registered earlier.
   * <pre>
   *   {@code
   *   serializer.registerDefault(Serializable.class, SerializableSerializer.class);
   *   serializer.register(SomeSerializable.class, 1);
   *   }
   * </pre>
   * If an object of a type that has not been {@link #register(Class) registered} is
   * {@link #writeObject(Object) serialized} and {@link #isWhitelistRequired() whitelisting} is disabled,
   * the object will be serialized with the class name and a default serializer if one is found.
   *
   * @param baseType The base type for which to register the default serializer. Types that extend the base
   *                 type and are registered without a specific {@link TypeSerializer} will be serialized
   *                 using the registered default {@link TypeSerializer}.
   * @param factory The default type serializer factory with which to serialize instances of the base type.
   * @return The serializer.
   * @throws NullPointerException if either argument is {@code null}
   */
  public Serializer registerDefault(Class<?> baseType, TypeSerializerFactory factory) {
    registry.registerDefault(baseType, factory);
    return this;
  }

  /**
   * Returns the underlying buffer allocator.
   *
   * @return The underlying buffer allocator.
   */
  public BufferAllocator allocator() {
    return allocator;
  }

  /**
   * Allocates a new buffer with an arbitrary initial capacity and unlimited maximum capacity.
   * <p>
   * The buffer will be allocated via the {@link BufferAllocator} provided to this instance's constructor.
   * If no {@code BufferAllocator} was provided, the default {@link UnpooledHeapAllocator} will
   * be used.
   *
   * @return The allocated buffer. This will have an initial capacity that is dependent on the underlying {@link BufferAllocator}.
   */
  public Buffer allocate() {
    return allocator.allocate();
  }

  /**
   * Allocates a new buffer with an initial and an unlimited maximum capacity.
   * <p>
   * The buffer will be allocated via the {@link BufferAllocator} provided to this instance's constructor.
   * If no {@code BufferAllocator} was provided, the default {@link UnpooledHeapAllocator} will
   * be used.
   *
   * @param capacity The buffer capacity.
   * @return The allocated buffer. This will have an initial capacity of the given {@code capacity}
   */
  public Buffer allocate(long capacity) {
    return allocator.allocate(capacity);
  }

  /**
   * Allocates a new buffer with a dynamic capacity.
   * <p>
   * The buffer will be allocated via the {@link BufferAllocator} provided to this instance's constructor.
   * If no {@code BufferAllocator} was provided, the default {@link UnpooledHeapAllocator} will
   * be used.
   *
   * @param initialCapacity The initial buffer capacity.
   * @param maxCapacity The maximum buffer capacity.
   * @return The allocated buffer. This will have an initial capacity of {@code initialCapacity} and a maximum capacity
   *         of {@code maxCapacity}
   */
  public Buffer allocate(long initialCapacity, long maxCapacity) {
    return allocator.allocate(initialCapacity, maxCapacity);
  }

  /**
   * Copies the given object.
   *
   * @param object The object to copy.
   * @param <T> The object type.
   * @return The copied object.
   */
  public <T> T copy(T object) {
    return readObject(writeObject(object).flip());
  }

  /**
   * Returns the serializer for the given type else {@code null} if no serializer or factory are registered for the 
   * {@code type}.
   */
  @SuppressWarnings("unchecked")
  private <T> TypeSerializer<T> getSerializer(Class<T> type) {
    TypeSerializer<T> serializer = (TypeSerializer<T>) serializers.get(type);
    if (serializer == null) {
      TypeSerializerFactory factory = registry.factory(type);
      if (factory != null) {
        serializer = (TypeSerializer<T>) factory.createSerializer(type);
        serializers.put(type, serializer);
      }
    }
    return serializer;
  }

  /**
   * Writes an object to a buffer.
   * <p>
   * Serialized bytes will be written to a {@link Buffer} allocated via the {@link BufferAllocator}
   * provided to this instance's constructor. Note that for consistency with {@link Serializer#writeObject(Object, Buffer)}
   * the returned buffer will not be flipped, so users should {@link Buffer#flip()} the buffer prior to reading.
   * <p>
   * The given object must have a {@link Serializer#register(Class) registered} serializer or implement {@link java.io.Serializable}.
   * If a serializable type ID was provided during registration, the type ID will be written to the returned
   * {@link Buffer} in lieu of the class name. Types with no associated type ID will be written
   * to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's {@link java.io.ObjectOutputStream}.
   * Types that implement {@link java.io.Externalizable} will be serialized via that interface's methods unless a custom
   * {@link TypeSerializer} has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws SerializationException If no serializer is registered for the object.
   * @see Serializer#writeObject(Object, Buffer)
   */
  public <T> Buffer writeObject(T object) {
    return writeObject(object, allocator.allocate());
  }

  /**
   * Writes an object to the given output stream.
   * <p>
   * The given object must have a {@link Serializer#register(Class) registered} serializer or implement {@link java.io.Serializable}.
   * If a serializable type ID was provided during registration, the type ID will be written to the given
   * {@link Buffer} in lieu of the class name. Types with no associated type ID will be written
   * to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link CatalystSerializable} will be serialized via
   * {@link CatalystSerializable#writeObject(BufferOutput, Serializer)} unless a
   * {@link TypeSerializer} was explicitly registered for the type.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's {@link java.io.ObjectOutputStream}.
   * Types that implement {@link java.io.Externalizable} will be serialized via that interface's methods unless a custom
   * {@link TypeSerializer} has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write.
   * @param outputStream The output stream to which to write the object.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws SerializationException If no serializer is registered for the object.
   * @see Serializer#writeObject(Object)
   */
  public <T> OutputStream writeObject(T object, OutputStream outputStream) {
    writeObject(object, new OutputStreamBufferOutput(outputStream));
    return outputStream;
  }

  /**
   * Writes an object to the given buffer.
   * <p>
   * Serialized bytes will be written to the given {@link Buffer} starting at its current
   * {@link Buffer#position()}. If the bytes {@link Buffer#remaining()} in
   * the buffer are not great enough to hold the serialized bytes, the buffer will be automatically expanded up to the
   * buffer's {@link Buffer#maxCapacity()}.
   * <p>
   * The given object must have a {@link Serializer#register(Class) registered} serializer or implement {@link java.io.Serializable}.
   * If a serializable type ID was provided during registration, the type ID will be written to the given
   * {@link Buffer} in lieu of the class name. Types with no associated type ID will be written
   * to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link CatalystSerializable} will be serialized via
   * {@link CatalystSerializable#writeObject(BufferOutput, Serializer)} unless a
   * {@link TypeSerializer} was explicitly registered for the type.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's {@link java.io.ObjectOutputStream}.
   * Types that implement {@link java.io.Externalizable} will be serialized via that interface's methods unless a custom
   * {@link TypeSerializer} has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write.
   * @param buffer The buffer to which to write the object.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws SerializationException If no serializer is registered for the object.
   * @see Serializer#writeObject(Object)
   */
  public <T> Buffer writeObject(T object, Buffer buffer) {
    writeObject(object, (BufferOutput<?>) buffer);
    return buffer;
  }

  /**
   * Writes an object to the given buffer.
   * <p>
   * Serialized bytes will be written to the given {@link Buffer} starting at its current
   * {@link Buffer#position()}. If the bytes {@link Buffer#remaining()} in
   * the buffer are not great enough to hold the serialized bytes, the buffer will be automatically expanded up to the
   * buffer's {@link Buffer#maxCapacity()}.
   * <p>
   * The given object must have a {@link Serializer#register(Class) registered} serializer or implement {@link java.io.Serializable}.
   * If a serializable type ID was provided during registration, the type ID will be written to the given
   * {@link Buffer} in lieu of the class name. Types with no associated type ID will be written
   * to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link CatalystSerializable} will be serialized via
   * {@link CatalystSerializable#writeObject(BufferOutput, Serializer)} unless a
   * {@link TypeSerializer} was explicitly registered for the type.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's {@link java.io.ObjectOutputStream}.
   * Types that implement {@link java.io.Externalizable} will be serialized via that interface's methods unless a custom
   * {@link TypeSerializer} has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write.
   * @param buffer The buffer to which to write the object.
   * @param <T> The object type.
   * @return The serialized object.
   * @throws SerializationException If no serializer is registered for the object.
   * @see Serializer#writeObject(Object)
   */
  public <T> BufferOutput<?> writeObject(T object, BufferOutput<?> buffer) {
    if (object == null) {
      return buffer.writeByte(Identifier.NULL.code());
    }

    Class<?> type = object.getClass();

    // Enums that implement interfaces or methods are generated as inner classes. For this reason,
    // we need to get the enclosing class if it's an enum.
    if (type.getEnclosingClass() != null && type.getEnclosingClass().isEnum())
      type = type.getEnclosingClass();

    // Look up the serializer for the given object type.
    TypeSerializer serializer = getSerializer(type);

    // If no serializer was found, throw a serialization exception.
    if (serializer == null) {
      throw new SerializationException("cannot serialize unregistered type: " + type);
    }

    // Cache the serializable type ID if necessary.
    if (!ids.containsKey(type)) {
      ids.put(type, registry.id(type));
    }

    // Lookup the serializable type ID for the type.
    int typeId = registry.id(type);
    if (typeId == 0) {
      return writeByClass(type, object, buffer, serializer);
    }
    return writeById(typeId, object, buffer, serializer);
  }

  /**
   * Writes an object to the buffer using the given serialization ID.
   */
  @SuppressWarnings("unchecked")
  private BufferOutput writeById(int id, Object object, BufferOutput output, TypeSerializer serializer) {
    for (Identifier identifier : Identifier.values()) {
      if (identifier.accept(id)) {
        identifier.write(id, output.writeByte(identifier.code()));
        serializer.write(object, output, this);
        return output;
      }
    }
    throw new SerializationException("invalid type ID: " + id);
  }

  /**
   * Writes an object to the buffer with its class name.
   */
  @SuppressWarnings("unchecked")
  private BufferOutput writeByClass(Class<?> type, Object object, BufferOutput output, TypeSerializer serializer) {
    if (whitelistRequired.get())
      throw new SerializationException("cannot serialize unregistered type: " + type);
    serializer.write(object, output.writeByte(Identifier.CLASS.code()).writeUTF8(type.getName()), this);
    return output;
  }

  /**
   * Reads an object from the given input stream.
   * <p>
   * During deserialization, the buffer will first be read to determine the type to be deserialized. If the object was
   * written using a serializable type ID, the given ID will be used to locate the serialized type. The type must have
   * been {@link Serializer#register(Class) registered} with this {@link Serializer} instance in order to
   * perform a reverse lookup.
   * <p>
   * If the type was written to the buffer with a fully qualified class name, the class name will be used to load the
   * object class via {@link Class#forName(String)}. Serializable types must implement a no-argument constructor to be
   * properly deserialized.
   * <p>
   * If the serialized type is an instance of {@link CatalystSerializable},
   * {@link CatalystSerializable#readObject(BufferInput, Serializer)} will be used to
   * read the object attributes from the buffer.
   * <p>
   * If the type is a {@link java.io.Serializable} type serialized with native Java serialization, it will be read from
   * the buffer via {@link java.io.ObjectInputStream}.
   * <p>
   * For types that implement {@link ReferenceCounted}, the serializer will use an internal object pool
   * to automatically pool and reuse reference counted types for deserialization. This means that users must release
   * {@link ReferenceCounted} types back to the object pool via
   * {@link ReferenceCounted#release()} or {@link ReferenceCounted#close()}
   * once complete.
   *
   * @param inputStream The input stream from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   * @throws SerializationException If no type could be read from the provided buffer.
   */
  public <T> T readObject(InputStream inputStream) {
    return readObject(new InputStreamBufferInput(inputStream));
  }

  /**
   * Reads an object from the given buffer.
   * <p>
   * The object will be read from the given buffer starting at the current {@link Buffer#position()}.
   * <p>
   * During deserialization, the buffer will first be read to determine the type to be deserialized. If the object was
   * written using a serializable type ID, the given ID will be used to locate the serialized type. The type must have
   * been {@link Serializer#register(Class) registered} with this {@link Serializer} instance in order to
   * perform a reverse lookup.
   * <p>
   * If the type was written to the buffer with a fully qualified class name, the class name will be used to load the
   * object class via {@link Class#forName(String)}. Serializable types must implement a no-argument constructor to be
   * properly deserialized.
   * <p>
   * If the serialized type is an instance of {@link CatalystSerializable},
   * {@link CatalystSerializable#readObject(BufferInput, Serializer)} will be used to
   * read the object attributes from the buffer.
   * <p>
   * If the type is a {@link java.io.Serializable} type serialized with native Java serialization, it will be read from
   * the buffer via {@link java.io.ObjectInputStream}.
   * <p>
   * For types that implement {@link ReferenceCounted}, the serializer will use an internal object pool
   * to automatically pool and reuse reference counted types for deserialization. This means that users must release
   * {@link ReferenceCounted} types back to the object pool via
   * {@link ReferenceCounted#release()} or {@link ReferenceCounted#close()}
   * once complete.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   * @throws SerializationException If no type could be read from the provided buffer.
   */
  public <T> T readObject(Buffer buffer) {
    return readObject((BufferInput<?>) buffer);
  }

  /**
   * Reads an object from the given buffer.
   * <p>
   * The object will be read from the given buffer starting at the current {@link Buffer#position()}.
   * <p>
   * During deserialization, the buffer will first be read to determine the type to be deserialized. If the object was
   * written using a serializable type ID, the given ID will be used to locate the serialized type. The type must have
   * been {@link Serializer#register(Class) registered} with this {@link Serializer} instance in order to
   * perform a reverse lookup.
   * <p>
   * If the type was written to the buffer with a fully qualified class name, the class name will be used to load the
   * object class via {@link Class#forName(String)}. Serializable types must implement a no-argument constructor to be
   * properly deserialized.
   * <p>
   * If the serialized type is an instance of {@link CatalystSerializable},
   * {@link CatalystSerializable#readObject(BufferInput, Serializer)} will be used to
   * read the object attributes from the buffer.
   * <p>
   * If the type is a {@link java.io.Serializable} type serialized with native Java serialization, it will be read from
   * the buffer via {@link java.io.ObjectInputStream}.
   * <p>
   * For types that implement {@link ReferenceCounted}, the serializer will use an internal object pool
   * to automatically pool and reuse reference counted types for deserialization. This means that users must release
   * {@link ReferenceCounted} types back to the object pool via
   * {@link ReferenceCounted#release()} or {@link ReferenceCounted#close()}
   * once complete.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   * @throws SerializationException If no type could be read from the provided buffer.
   */
  @SuppressWarnings("unchecked")
  public <T> T readObject(BufferInput<?> buffer) {
    int code = buffer.readByte();
    Identifier identifier = Identifier.forCode(code);
    switch (identifier) {
      case NULL:
        return null;
      case CLASS:
        return readByClass(buffer);
      default:
        return readById(identifier.read(buffer), buffer);
    }
  }

  /**
   * Reads a serializable object.
   *
   * @param id The serializable type ID.
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   */
  @SuppressWarnings("unchecked")
  private <T> T readById(int id, BufferInput<?> buffer) {
    Class<T> type = (Class<T>) registry.type(id);
    if (type == null)
      throw new SerializationException("cannot deserialize: unknown type");

    TypeSerializer<T> serializer = getSerializer(type);
    if (serializer == null)
      throw new SerializationException("cannot deserialize: unknown type");

    return serializer.read(type, buffer, this);
  }

  /**
   * Reads a writable object.
   *
   * @param buffer The buffer from which to read the object.
   * @param <T> The object type.
   * @return The read object.
   */
  @SuppressWarnings("unchecked")
  private <T> T readByClass(BufferInput<?> buffer) {
    String name = buffer.readUTF8();
    if (whitelistRequired.get())
      throw new SerializationException("cannot deserialize unregistered type: " + name);

    Class<T> type = (Class<T>) types.get(name);
    if (type == null) {
      try {
        type = (Class<T>) Class.forName(name);
        if (type == null)
          throw new SerializationException("cannot deserialize: unknown type");
        types.put(name, type);
      } catch (ClassNotFoundException e) {
        throw new SerializationException("object class not found: " + name, e);
      }
    }
    
    TypeSerializer<T> serializer = getSerializer(type);
    if (serializer == null)
      throw new SerializationException("cannot deserialize unregistered type: " + name);

    return serializer.read(type, buffer, this);
  }

  /**
   * Clones the object.
   */
  public final Serializer clone() {
    return new Serializer(registry, allocator, whitelistRequired);
  }

}
