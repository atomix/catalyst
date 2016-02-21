# Catalyst

[![Build Status](https://travis-ci.org/atomix/catalyst.svg)](https://travis-ci.org/atomix/catalyst)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix.catalyst/catalyst-parent/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.atomix.catalyst%22) 

### [Website][Website] • [Google Group][Google group] • [Javadoc][Javadoc] • [Atomix][Atomix] • [Copycat][Copycat]

Catalyst is an I/O and serialization framework designed for use in [Atomix][Atomix], [Copycat][Copycat]
and related projects. It provides high-level abstractions for common storage and networking facilities.

#### I/O
Catalyst provides a buffer abstraction over on-heap and off-heap memory, memory mapped files, and `RandomAccessFile`:

```java
Buffer buffer = HeapBuffer.allocate(128);
buffer.writeLong(1).writeByte(2);
```

#### Messaging
Catalyst provides an abstraction for asynchronous message-based networking that allows different frameworks to be substituted for
communication in Copycat and Atomix:

```java
Transport transport = NettyTransport.builder()
  .withThreads(4)
  .build();

Client client = transport.client();
client.connect(new Address("localhost", 8888)).thenAccept(connection -> {
  connection.send("Hello world!");
});
```

#### Serialization
Catalyst provides a binary serialization abstraction designed to support a variety of frameworks and use cases. Serializers include
support for primitives, collections, `Serializable`, `Externalizable`, and [Kryo](https://github.com/EsotericSoftware/kryo) and
[Jackson](http://wiki.fasterxml.com/JacksonHome) serializers.

```java
Serializer serializer = new Serializer();
serializer.register(MyJacksonSerializable.class, GenericJacksonSerializer.class);

Buffer buffer = serializer.writeObject(new MyJacksonSerializable());
buffer.flip();
MyJacksonSerializable object = serializer.readObject(buffer);
```

*For more extensive documentation see the [website][Website]*

[Website]: http://atomix.io/catalyst/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.io/catalyst/api/latest/
[Atomix]: http://github.com/atomix/atomix
[Copycat]: http://github.com/atomix/copycat
