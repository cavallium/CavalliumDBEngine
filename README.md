CavalliumDB Engine
==================
![Maven Package](https://github.com/Cavallium/CavalliumDBEngine/workflows/Maven%20Package/badge.svg)

A very simple reactive wrapper for RocksDB and Lucene.

This is not a database, but only a wrapper for Lucene Core and RocksDB, with a bit of abstraction.

# Features
- **RocksDB key-value database engine**
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
  
  Since RocksDB and lucene indices are different software, you can't take a snapshot of everything in the same instant.
  
  A single snapshot must be implemented as a collection of all the snapshots.
  
- **Is CavalliumDB Engine suitable for your project?**
  
  No.
  
  This engine is largely undocumented, and it doesn't provide extensive tests.

# Examples

In `src/example/java` you can find some quick implementations of each core feature.
