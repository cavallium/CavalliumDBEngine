CavalliumDB Engine
==================

A very simple wrapper for RocksDB and Lucene, with gRPC and direct connections.

This is not a database, but only a wrapper for Lucene Core and RocksDB, with a bit of abstraction.

# Features
## RocksDB Key-Value NoSQL database engine
- Snapshots
- Multi-column databases
- WAL and corruption recovery strategies
- Multiple data types:
  - Bytes (Singleton)
  - Maps of bytes (Dictionary)
  - Maps of maps of bytes (Deep dictionary)
  - Sets of bytes (Dictionary without values)
  - Maps of sets of bytes (Deep dictionary without values)

## Apache Lucene Core indexing library
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
- Snapshots

# F.A.Q.
## Why is it so difficult?
This is not a DMBS.

This is an engine on which a DBMS can be built upon. For this reason it's very difficult to use it directly without using it through abstraction layers.

## Can I use objects in the database?
Yes you must serialize/deserialize them using a library of your choice.

## Why there is a snapshot function for each database part?
Since RocksDB and lucene indices are different instances, every instance has its own snapshot function.

To have a single snapshot you must implement it as a collection of sub-snapshots in your DBMS.

## Is CavalliumDB Engine suitable for your project?
No.
This engine is largely undocumented, and it doesn't provide extensive tests on its methods.
