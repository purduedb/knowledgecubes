![KNOWLEDGECUBES_LOGO](src/main/resources/logo-svg.png)

## About

A Knowledge Cube, or KC for short, is a semantically-guided data management architecture, where data management is influenced by the data semantics rather than by a predefined scheme. KC relies on semantics to define how the data is fetched, organized, stored, optimized, and queried. Knowledge cubes use RDF to store data. This allows knowledge cubes to store Linked Data from the Web of Data. Knowledge cubes are envisioned to break down the centralized
architecture into multiple specialized cubes, each having its own index and data store.


## WORQ: Workload-Driven RDF Query Processing

KC uses a workload-driven RDF query processing technique, or WORQ for short, for filtering non-matching entries during join evaluation as early as possible to reduce the communication and computation overhead. WORQ generates a reduced sets of triples (or reductions, for short) to represent join pattern(s) of query workloads. WORQ can materialize the reductions on disk or in memory and reuses the reductions that share the same join pattern(s) to answer queries. Furthermore, these reductions are not computed beforehand, but are rather computed in an online fashion. KC also answer complex analytical queries that involve unbound properties. Based on a realization of KC on top of Spark, extensive experimentation demonstrates an order of magnitude enhancement in terms of preprocessing, storage, and query performance compared to the state-of-the-art cloud-based solutions.

## Features

* A spark-based API for SPARQL querying
* Efficient execution of frequent workload join patterns
* Materialze workload join patterns in memory or on disk
* Efficiently answer unbound property querying

## Usage

KC provide spark-based API for issuing RDF related operations.

#### Creating an RDF Store

KC provide the Store class for creation of an RDF store. The input to the store is a spark session, database path where the RDF dataset will be stores, and a local configuration path

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

#### Constructing Filters

KC provides exact and approximate structures for filtering data. Currently KC supports ```GEFIType.BLOOM```, ```GEFIType.ROARING```, and ```GEFIType.BITSET```.

```scala
import org.apache.spark.sql.SparkSession

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.GEFI.join.GEFIJoinCreator
import edu.purdue.knowledgecubes.utils.Timer

val spark = SparkSession.builder
            .appName(s"KnowledgeCubes Filter Creator")
            .getOrCreate()
            
var localPath = "/home/amadkour/projects/knowledgecubes/output/bio2rdf/"
var dbPath = "hdfs://bigdata1-vm1:8020/user/amadkour/bio2rdf-db/"
```

#### SPARQL Querying

KC provides a SPARQL query processor that takes as input the spark session, database path of where the RDF dataset was created, local configuration file path, a filter type, and a false postivie rate (if any).

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

val query =
  """
    SELECT ?GivenName ?FamilyName WHERE{
        ?p <http://yago-knowledge.org/resource/hasGivenName> ?GivenName . 
        ?p <http://yago-knowledge.org/resource/hasFamilyName> ?FamilyName . 
        ?p <http://yago-knowledge.org/resource/wasBornIn> ?city . 
        ?p <http://yago-knowledge.org/resource/hasAcademicAdvisor> ?a .
        ?a <http://yago-knowledge.org/resource/wasBornIn> ?city .
    }
  """.stripMargin

// Returns a Spark DataFrame containing the results
val r = queryProcessor.sparql(query)

```

## Upcoming Features

* Spatiotemporal queries
    * **Example:** Range, Distance Join
* Matrix-based operations over RDF data
    * Process graph algorithms
        * **Example:** Page Rank
    * Process natural language queries
        * **Example:** Similarity, Relatedness
* Web-based UI querying
    * **Example:** Send SPARQL queries to spark, view spatial results
    
## Publications

* Amgad Madkour, Ahmed M. Aly, Walid G. Aref, "WORQ: Workload-Driven RDF Query Processing", ISWC 2018 \[To Appear\]

* Amgad Madkour, Walid G. Aref, Ahmed M. Aly, “SPARTI: Scalable RDF Data Management Using Query-Centric Semantic Partitioning”, Semantic Big Data (SBD18)

* Amgad Madkour, Walid G. Aref, Sunil Prabhakar, Mohamed Ali, Siarhei Bykau, “TrueWeb: A Proposal for Scalable Semantically-Guided Data Management and Truth Finding in Heterogeneous Web Sources”, Semantic Big Data (SBD18)

* Amgad Madkour, Walid G. Aref, Saleh Basalamah, “Knowledge Cubes - A Proposal for Scalable and Semantically-Guided Management of Big Data”, IEEE BigData 2013
