# CavalliumDB Engine

![Maven Package](https://github.com/Cavallium/CavalliumDBEngine/workflows/Maven%20Package/badge.svg)

[Reactive](https://www.reactive-streams.org/) database engine written in Java (17+) using [Project Reactor](https://github.com/reactor/reactor-core).

## DO NOT USE THIS PROJECT: THIS IS A PERSONAL PROJECT, THE API IS NOT STABLE, THE CODE IS NOT TESTED.

This library provides a basic reactive abstraction and implementation of a **key-value store** and a **search engine**.

Four implementations exists out-of-the-box, two for the key-value store, two for the search engine, but it's possible to add more implementations.

## Key-value store implementations 

1. [RocksDB](https://github.com/facebook/rocksdb): A persistent key-value store for flash storage
3. [ConcurrentSkipListMap](https://docs.oracle.com/en/java/javase/16/docs/api/java.base/java/util/concurrent/ConcurrentSkipListMap.html): A concurrent in-memory key-value store

## Search engine implementations

1. Persistent [Lucene Core](https://github.com/apache/lucene) with custom sharding: Featureful and fast text search engine library
2. In-memory temporary [Lucene Core](https://github.com/apache/lucene) instance: Useful for building and analyzing temporary indices

## Extra features

### Serializable search engine queries

Queries can be serialized and deserialized using an efficient custom serialization format

### Direct byte buffer

The database abstraction can avoid copying the data multiple times by using RocksDB JNI and Netty 5 buffers

### Declarative data records generator and versioned codecs

A data generator that generates [Java 16 records](https://www.baeldung.com/java-record-keyword) is available:
it allows you to generate custom records by defining the fields using a .yaml file.

The generator also generates at compile time the source of specialized serializers,
deserializers, and upgraders, for each custom record.

The key-value store abstraction allows you to deserialize old versions of your data transparently, by using
the custom upgraders and the custom deserializers automatically.

The data generator can be found in the [Data generator](https://github.com/Cavallium/data-generator) repository.

# Features
- **RocksDB key-value store**
  - Snapshots
  - Multi-column database
  - Write-ahead log and corruption recovery
  - Multiple data types:
    - Single value (Singleton)
    - Map (Dictionary)
    - Composable nested map (Deep dictionary)
  - Customizable data serializers
  - Values codecs
  - Update-on-write value versioning using versioned codecs
  
- **Apache Lucene Core indexing library**
  - Snapshots
  - Documents structure
  - Sorting
    - Ascending and descending
    - Numeric or non-numeric
  - Searching
    - Nested search terms
    - Combined search terms
    - Fuzzy text search
    - Coordinates, integers, longs, strings, text
  - Indicization and analysis
    - N-gram
    - Edge N-gram
    - English words
    - Stemming
    - Stopwords removal
  - Results filtering

# F.A.Q.
- **Why is it so difficult to use?**
  
  This is not a DBMS.
  
  This is an engine on which a DBMS can be built upon; for this reason it's very difficult to use directly without building another abstraction layer on top.
  
- **Can I use objects instead of byte arrays?**
  
  Yes, you must serialize/deserialize them using a library of your choice.
  
  CodecSerializer allows you to implement versioned data using a codec for each data version.
  Note that it uses 1 to 4 bytes more for each value to store the version.
  
- **Why there is a snapshot function for each database part?**
  
  Since RocksDB and lucene indices are different libraries, you can't take a snapshot of every database atomically.
  
  An universal snapshot must be implemented as a collection of each database snapshot.
  
- **Is CavalliumDB Engine suitable for your project?**
  
  No.
  
  This engine is largely undocumented, and it doesn't provide extensive tests.

# Examples

In `src/example/java` you can find some *(ugly)* examples.
