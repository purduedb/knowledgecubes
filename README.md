![KNOWLEDGECUBES_LOGO](src/main/resources/logo-svg.png)

## About

Cloud-based systems can be used to manage web-scale RDF data. However, operations that involve complex joins introduce several performance challenges, e.g., communication and computation overhead. To alleviate these challenges, Knowledge Cubes, an RDF system that filters non-matching intermediate results during join evaluation as early as possible to reduce the communication and computation overhead. Knowledge Cubes uses a filter-based approach to generate reduced sets of triples (or reductions, for short) to represent join pattern(s) of query workloads. Knowledge Cubes can materialize the reductions on disk or in memory and reuses the reductions that share the same join pattern(s) to answer queries. Furthermore, these reductions are not computed beforehand, but are rather computed in an online fashion. Knowledge Cubes also answer complex analytical queries that involve unbound properties. Based on a realization of Knowledge Cubes on top of Spark, extensive experimentation demonstrates an order of magnitude enhancement in terms of preprocessing, storage, and query performance compared to the state-of-the-art cloud-based solutions.

## Features

* A spark-based API for SPARQL querying
* Efficient execution of frequent workload join patterns
* Materialze workload join patterns in memory or on disk
* Efficiently answer unbound property querying

## Usage

Knowledge Cubes provide spark-based API for issuing RDF related operations.

#### Creating an RDF Store

Knowledge Cubes provide the Store class for creation of an RDF store. The input to the store is a spark session, database path where the RDF dataset will be stores, and a local configuration path

```scala
import org.apache.spark.sql.SparkSession

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.GEFI.join.GEFIJoinCreator
import edu.purdue.knowledgecubes.storage.persistent.Store

val localPath = "/path/to/local/path"
val dbPath = "/path/to/db/path"
val ntPath = "/path/to/rdf/file"

val spark = SparkSession.builder
            .appName(s"KnowledgeCubes Store Creator")
            .getOrCreate()

val store = Store(spark, dbPath, localPath)
store.create(ntPath)
```

#### SPARQL Querying

Knowledge Cubes provides a SPARQL query processor that takes as input the spark session, database path of where the RDF dataset was created, local configuration file path, a filter type, and a false postivie rate (if any).

```scala
import org.apache.spark.sql.SparkSession

import edu.purdue.knowledgecubes.queryprocessor.QueryProcessor
import edu.purdue.knowledgecubes.GEFI.GEFIType

val spark = SparkSession.builder
            .appName(s"Knowledge Cubes Query")
            .getOrCreate()

val localPath = "/path/to/local/path"
val dbPath = "/path/to/db/path"
val filterType = GEFIType.ROARING // Roaring bitmap
val falsePositiveRate = 0

val queryProcessor = QueryProcessor(spark, dbPath, localPath, filterType, falsePositiveRate)
```


### Planned Features

* Spatial queries support
    * **Example:** Range, Distance Join
* Temporal queries support
* Perform matrix-based operations over RDF data
    * Process graph algorithms
        * **Example:** Page Rank
    * Process natural language queries
        * **Example:** Similarity, Relatedness
* Issue RDF queries via web-based UI
    * **Example:** Send SPARQL queries to spark, view spatial results
    
### Wish List

* Cache eviction algorithms (e.g., LFU,MRU etc.)
* Data Partition algorithms (e.g., Adaptive)

### Publications

* Amgad Madkour, Walid G. Aref, Saleh Basalamah, “Knowledge Cubes - A Proposal for Scalable and Semantically-Guided Management of Big Data”, IEEE BigData 2013

* Amgad Madkour, Walid G. Aref, Ahmed M. Aly, “SPARTI: Scalable RDF Data Management Using Query-Centric Semantic Partitioning”, Semantic Big Data (SBD18)

* Amgad Madkour, Walid G. Aref, Sunil Prabhakar, Mohamed Ali, Siarhei Bykau, “TrueWeb: A Proposal for Scalable Semantically-Guided Data Management and Truth Finding in Heterogeneous Web Sources”, Semantic Big Data (SBD18)