package edu.purdue.knowledgecubes.queryprocessor.executor

import scala.collection.mutable.{HashSet, ListBuffer}

import com.typesafe.scalalogging.Logger
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Op, OpWalker}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.GEFI.join.GEFIJoin
import edu.purdue.knowledgecubes.metadata.{Catalog, Result}
import edu.purdue.knowledgecubes.partition.Partition
import edu.purdue.knowledgecubes.queryprocessor.parser.QueryVisitor
import edu.purdue.knowledgecubes.rdf.{RDFPropertyIdentifier, RDFTriple}
import edu.purdue.knowledgecubes.storage.cache.{CacheEntry, CacheManager}
import edu.purdue.knowledgecubes.storage.cache.CacheEntryType._
import edu.purdue.knowledgecubes.utils.PrefixHandler


class Executor(catalog: Catalog) {

  val LOG = Logger(LoggerFactory.getLogger(classOf[Executor]))
  import catalog.spark.implicits._

  def run(op: Op): Result = {
    // Visit BGP
    val opRoot = op
    val visitor = new QueryVisitor()
    OpWalker.walk(opRoot, visitor)

    // Get visitor information
    val bgpTriples = visitor.triples.map(x => PrefixHandler.parseBaseURI(x))
    val numTriples = visitor.triples.length
    val queryJoins = Map(
      visitor.joinVariables.mapValues(v =>
        v.map(t => PrefixHandler.parseBaseURI(t))).toSeq: _*)
    val numUnboundTriples = visitor.unboundPropertyTriples.length
    val projectionList = visitor.computeProjectionList(op)
    val total = numUnboundTriples + numTriples

    // Will hold all loaded data
    var dataFrames = Map[Triple, Dataset[RDFTriple]]()
    var isCached = false
    var tableSizes: Long = 0
    var reductionSizes: Long = 0
    var loadTime: Long = 0
    var filterTime: Long = 0

    for (triplePattern <- bgpTriples) {
      if (triplePattern.getPredicate.isVariable) {
        val property = new RDFPropertyIdentifier(catalog)
        val properties = property.identify(triplePattern, queryJoins)
        var dataFrame: Dataset[RDFTriple] = catalog.spark.emptyDataset[RDFTriple]
        for(propName <- properties) {
          val join = new GEFIJoin(catalog)
          val joinFilters = join.identify(propName, triplePattern, queryJoins)
          val (tbl, cached, numTuples, lTime, fTime) = loadData(propName, joinFilters)
          if (!isCached) {
            isCached = cached
          }
          if(dataFrame.count == 0) {
            dataFrame = tbl
          } else {
            dataFrame = dataFrame.union(tbl)
          }
          reductionSizes += numTuples
          loadTime += lTime
          filterTime += fTime
          tableSizes += catalog.tablesInfo(propName)("numTuples").toLong
        }
        dataFrames += (triplePattern -> dataFrame)
      } else {
        val propName = triplePattern.getPredicate.toString
        val join = new GEFIJoin(catalog)
        val joinFilters = join.identify(propName, triplePattern, queryJoins)
        val (tbl, cached, numTuples, lTime, fTime) = loadData(propName, joinFilters)
        if (!isCached) {
          isCached = cached
        }
        reductionSizes += numTuples
        loadTime += lTime
        filterTime += fTime
        tableSizes += catalog.tablesInfo(propName)("numTuples").toLong
        dataFrames += (triplePattern -> tbl)
      }
    }

    // Time the query
    val start = System.currentTimeMillis
    val evaluated = evaluateBGP(bgpTriples, dataFrames)
    val result = evaluated.select(projectionList: _*)
    val numResults = result.count
    val time = System.currentTimeMillis - start

    var maxJoins = 0
    for ((prop, triples) <- queryJoins) {
      val numJoins = triples.size
      if (numJoins > maxJoins) {
        maxJoins = numJoins
      }
    }
    new Result(result,
      projectionList.toList,
      numResults,
      tableSizes,
      reductionSizes,
      maxJoins,
      isCached,
      total,
      time,
      loadTime,
      filterTime)
  }

  def loadData(propName: String,
               queryFilters: Map[String, String]): Tuple5[Dataset[RDFTriple], Boolean, Long, Long, Long] = {
    // The three returned values
    var table: Dataset[RDFTriple] = catalog.spark.emptyDataset[RDFTriple]
    var isReductionWarm = false
    var numTuples: Long = 0
    var loadTime: Long = 0
    var filterTime: Long = 0

    if (queryFilters.nonEmpty && catalog.filterType != GEFIType.NONE) {

      if ((queryFilters.keySet.contains("s") && queryFilters.keySet.contains("o") &&
        catalog.joinReductionsInfo.contains(queryFilters("s")) &&
        catalog.joinReductionsInfo.contains(queryFilters("o")) &&
        catalog.joinReductionsInfo(queryFilters("s")) == 0 &&
        catalog.joinReductionsInfo(queryFilters("o")) == 0) ||
        (queryFilters.size == 1 &&
          (queryFilters.keySet.contains("s") &&
          catalog.joinReductionsInfo.contains(queryFilters("s")) &&
          catalog.joinReductionsInfo(queryFilters("s")) == 0)) ||
        (queryFilters.size == 1 &&
          queryFilters.keySet.contains("o") &&
          catalog.joinReductionsInfo.contains(queryFilters("o")) &&
          catalog.joinReductionsInfo(queryFilters("o")) == 0)) {
          return (table, isReductionWarm, numTuples, loadTime, filterTime)
      }

      // Compute or load reduction
      if (queryFilters.keySet.contains("s") &&
        queryFilters.keySet.contains("o") &&
        CacheManager.contains(queryFilters("s")) &&
        CacheManager.contains(queryFilters("o"))) {
        // Current Policy: Load reduction based on its size | Alternatily: Prefer subject
        if (CacheManager.get(queryFilters("s")).size < CacheManager.get(queryFilters("o")).size) {
          table = CacheManager.get(queryFilters("s")).data
          numTuples = CacheManager.get(queryFilters("s")).size
        } else {
          table = CacheManager.get(queryFilters("o")).data
          numTuples = CacheManager.get(queryFilters("o")).size
        }
        isReductionWarm = true
      } else if (queryFilters.keySet.contains("s") && CacheManager.contains(queryFilters("s"))) {
        table = CacheManager.get(queryFilters("s")).data
        numTuples = CacheManager.get(queryFilters("s")).size
        isReductionWarm = true
      } else if (queryFilters.keySet.contains("o") && CacheManager.contains(queryFilters("o"))) {
        table = CacheManager.get(queryFilters("o")).data
        numTuples = CacheManager.get(queryFilters("o")).size
        isReductionWarm = true
      } else {
        // Check the catalog if the reduction exists
        if (queryFilters.keySet.contains("s") &&
          queryFilters.keySet.contains("o") &&
          catalog.joinReductionsInfo.contains(queryFilters("s")) &&
          catalog.joinReductionsInfo(queryFilters("s")) > 0 &&
          catalog.joinReductionsInfo.contains(queryFilters("o")) &&
          catalog.joinReductionsInfo(queryFilters("o")) > 0 ) {
          // Current Policy: Load reduction based on its size | Alternatily: Prefer subject
          if (catalog.joinReductionsInfo(queryFilters("s")) < catalog.joinReductionsInfo(queryFilters("o"))) {
            table = catalog.spark.read.parquet(catalog.joinReductionsPath + queryFilters("s")).as[RDFTriple]
            numTuples = table.count()
          } else {
            table = catalog.spark.read.parquet(catalog.joinReductionsPath + queryFilters("o")).as[RDFTriple]
            numTuples = table.count()
          }
          isReductionWarm = true
        } else if (queryFilters.keySet.contains("s") &&
          catalog.joinReductionsInfo.contains(queryFilters("s")) &&
          catalog.joinReductionsInfo(queryFilters("s")) > 0) {
          table = catalog.spark.read.parquet(catalog.joinReductionsPath + queryFilters("s")).as[RDFTriple]
          numTuples = table.count()
          isReductionWarm = true
        } else if (queryFilters.keySet.contains("o") &&
          catalog.joinReductionsInfo.contains(queryFilters("o")) &&
          catalog.joinReductionsInfo(queryFilters("o")) > 0) {
          table = catalog.spark.read.parquet(catalog.joinReductionsPath + queryFilters("o")).as[RDFTriple]
          numTuples = table.count()
          isReductionWarm = true
        } else {
          if (!CacheManager.contains(propName)) {
            val (tbl, num, lTime) = loadOriginal(propName)
            table = tbl
            numTuples = num
            loadTime += lTime
          } else {
            table = CacheManager.get(propName).data
          }
          // Compute the reduction
          if (queryFilters.keySet.contains("s")) {
            val (tbl, num, fTime) = loadReduction(propName, "s", queryFilters("s"), table)
            table = tbl
            numTuples = num
            filterTime += fTime
            CacheManager.add(new CacheEntry(queryFilters("s"), numTuples, JOIN_REDUCTION, table))
          }
          if (queryFilters.keySet.contains("o") && !queryFilters.keySet.contains("s")) {
            val (tbl, num, fTime) = loadReduction(propName, "o", queryFilters("o"), table)
            table = tbl
            numTuples = num
            filterTime += fTime
            CacheManager.add(new CacheEntry(queryFilters("o"), numTuples, JOIN_REDUCTION, table))
          }
          isReductionWarm = false
        }
      }
    } else {
      val (tbl, num, lTime) = loadOriginal(propName)
      table = tbl
      numTuples = num
      loadTime += lTime
    }
    (table, isReductionWarm, numTuples, loadTime, filterTime)
  }

  def loadReduction(propName: String,
                    column: String,
                    filterName: String,
                    orig: Dataset[RDFTriple]): Tuple3[Dataset[RDFTriple], Long, Long] = {
    val join = new GEFIJoin(catalog)
    val reducedEntries = join.compute(propName, column, filterName, orig)
    val start = System.currentTimeMillis()
    val numTuples = reducedEntries.count()
    val filterTime = System.currentTimeMillis() - start
    LOG.debug(s"Filtering Time: $filterTime")
    val table = Partition.byJoinAttribute(column, reducedEntries)
    table.cache()
    (table, numTuples, filterTime)
  }

  def loadOriginal(propName: String): Tuple3[Dataset[RDFTriple], Long, Long] = {
    val spark = catalog.spark
    val numTuples = catalog.tablesInfo(propName)("numTuples").toLong
    if (!CacheManager.contains(propName)) {
      val path = catalog.dataPath + catalog.tablesInfo(propName)("tableName")
      var table = spark.read.parquet(path).as[RDFTriple]
      table = Partition.byDefaultCriteria(table)
      table.cache()
      // Force reading the data //////////////////////
      val start = System.currentTimeMillis()
      table.count()
      val loadTime = System.currentTimeMillis() - start
      LOG.debug(s"Load Time: $loadTime")
      /////////////////////////////////////////////////
      CacheManager.add(new CacheEntry(propName, numTuples, ORIGINAL, table))
      (table, numTuples, loadTime)
    } else {
      val table = CacheManager.get(propName).data
      (table, numTuples, 0)
    }
  }

  def joinTwoVariables(variables: HashSet[String],
                       rootTbl: DataFrame,
                       var1: String,
                       var2: String,
                       otherTbl: DataFrame): DataFrame = {
    var rootTable = rootTbl
    var otherTable = otherTbl
    if(variables.contains(var1) && variables.contains(var2)) {
      rootTable = joinTwoUniqueColumns(rootTable, var1, var2, otherTable)
    } else if (variables.contains(var1)) {
      rootTable = joinOneUniqueColumn(rootTable, var1, otherTable)
    } else {
      rootTable = joinOneUniqueColumn(rootTable, var2, otherTable)
    }
    rootTable
  }

  def evaluateBGP(queryTriples: ListBuffer[Triple], dataFrames: Map[Triple, Dataset[RDFTriple]]): DataFrame = {
    // Process first triple
    var variables = HashSet[String]()
    var rootTable = dataFrames(queryTriples.head).toDF()
    val t1 = queryTriples.head

    rootTable = evaluateTriple(t1, rootTable)
    variables ++= identifyVariables(t1)

    val remainingTriples = queryTriples - t1
    for (otherTriple <- remainingTriples) {
      val t2 = otherTriple
      var otherTable = dataFrames(otherTriple).toDF()

      otherTable = evaluateTriple(t2, otherTable)

      if (t2.getSubject.isVariable && t2.getPredicate.isVariable && t2.getObject.isVariable) { // 0 0 0
        rootTable = joinThreeVariables(variables,
                                        rootTable,
                                        t2.getSubject.toString(),
                                        t2.getPredicate.toString(),
                                        t2.getObject.toString(),
                                        otherTable)
      } else if (t2.getSubject.isVariable && t2.getPredicate.isVariable && !t2.getObject.isVariable) { // 0 0 1
        rootTable = joinTwoVariables(variables,
                                      rootTable,
                                      t2.getSubject.toString(),
                                      t2.getPredicate.toString(),
                                      otherTable)
      } else if (t2.getSubject.isVariable && !t2.getPredicate.isVariable && t2.getObject.isVariable) { // 0 1 0
        rootTable = joinTwoVariables(variables,
                                      rootTable,
                                      t2.getSubject.toString(),
                                      t2.getObject.toString(),
                                      otherTable)
      } else if (t2.getSubject.isVariable && !t2.getPredicate.isVariable && !t2.getObject.isVariable) { // 0 1 1
        rootTable = joinOneUniqueColumn(rootTable,
                                        t2.getSubject.toString(),
                                        otherTable)
      } else if (!t2.getSubject.isVariable && t2.getPredicate.isVariable && t2.getObject.isVariable) { // 1 0 0
        rootTable = joinTwoVariables(variables,
                                        rootTable,
                                        t2.getPredicate.toString(),
                                        t2.getObject.toString(),
                                        otherTable)
      } else if (!t2.getSubject.isVariable && t2.getPredicate.isVariable && !t2.getObject.isVariable) { // 1 0 1
        rootTable = joinOneUniqueColumn(rootTable,
                                        t2.getPredicate.toString(),
                                        otherTable)
      } else if (!t2.getSubject.isVariable && !t2.getPredicate.isVariable && t2.getObject.isVariable) { // 1 1 0
        rootTable = joinOneUniqueColumn(rootTable,
                                        t2.getObject.toString(),
                                        otherTable)
      } else { // 1 1 1
        rootTable = rootTable.join(otherTable)
      }
      variables ++= identifyVariables(t2)
    }
    rootTable
  }

  def joinThreeVariables(variables: HashSet[String],
                         rootTbl: DataFrame,
                         var1: String,
                         var2: String,
                         var3: String,
                         otherTbl: DataFrame): DataFrame = {
    var rootTable = rootTbl
    var otherTable = otherTbl
    if(variables.contains(var1) && variables.contains(var2) && variables.contains(var3)) {
      rootTable = joinThreeUniqueColumns(rootTable, var1, var2, var3, otherTable)
    } else if (variables.contains(var1) && variables.contains(var2)) {
      rootTable = joinTwoUniqueColumns(rootTable, var1, var2, otherTable)
    } else if (variables.contains(var1) && variables.contains(var3)) {
      rootTable = joinTwoUniqueColumns(rootTable, var1, var3, otherTable)
    } else if (variables.contains(var2) && variables.contains(var3)) {
      rootTable = joinTwoUniqueColumns(rootTable, var2, var3, otherTable)
    } else if (variables.contains(var1)) {
      rootTable = joinOneUniqueColumn(rootTable, var1, otherTable)
    } else if (variables.contains(var2)) {
      rootTable = joinOneUniqueColumn(rootTable, var2, otherTable)
    } else {
      rootTable = joinOneUniqueColumn(rootTable, var3, otherTable)
    }
    rootTable
  }

  def joinThreeUniqueColumns(rootTbl: DataFrame,
                             col1: String,
                             col2: String,
                             col3: String,
                             otherTbl: DataFrame): DataFrame = {
    var rootTable = rootTbl
    var otherTable = otherTbl
    rootTable = rootTable.withColumnRenamed(col1, "t1")
    rootTable = rootTable.withColumnRenamed(col2, "t3")
    rootTable = rootTable.withColumnRenamed(col3, "t5")
    otherTable = otherTable.withColumnRenamed(col1, "t2")
    otherTable = otherTable.withColumnRenamed(col2, "t4")
    otherTable = otherTable.withColumnRenamed(col3, "t6")

    rootTable = rootTable.join(otherTable, otherTable.col("t2").equalTo(rootTable.col("t1")).
                                          and(otherTable.col("t4").equalTo(rootTable.col("t3").
                                          and(otherTable.col("t6").equalTo(rootTable.col("t5"))))))

    rootTable = rootTable.drop("t2")
    rootTable = rootTable.drop("t4")
    rootTable = rootTable.drop("t6")
    rootTable = rootTable.withColumnRenamed("t1", col1)
    rootTable = rootTable.withColumnRenamed("t3", col2)
    rootTable = rootTable.withColumnRenamed("t5", col3)
    rootTable
  }

  def joinOneUniqueColumn(rootTbl: DataFrame, col1: String, otherTbl: DataFrame): DataFrame = {
    var rootTable = rootTbl
    var otherTable = otherTbl
    rootTable = rootTable.withColumnRenamed(col1, "t1")
    otherTable = otherTable.withColumnRenamed(col1, "t2")
    rootTable = rootTable.join(otherTable, otherTable.col("t2").equalTo(rootTable.col("t1")))
    rootTable = rootTable.drop("t2")
    rootTable = rootTable.withColumnRenamed("t1", col1)
    rootTable
  }

  def joinTwoUniqueColumns(rootTbl: DataFrame, col1: String, col2: String, otherTbl: DataFrame): DataFrame = {
    var rootTable = rootTbl
    var otherTable = otherTbl
    rootTable = rootTable.withColumnRenamed(col1, "t1")
    rootTable = rootTable.withColumnRenamed(col2, "t3")
    otherTable = otherTable.withColumnRenamed(col1, "t2")
    otherTable = otherTable.withColumnRenamed(col2, "t4")

    rootTable = rootTable.join(otherTable,
      otherTable.col("t2").equalTo(rootTable.col("t1")).
        and(otherTable.col("t4").equalTo(rootTable.col("t3"))))

    rootTable = rootTable.drop("t2")
    rootTable = rootTable.drop("t4")
    rootTable = rootTable.withColumnRenamed("t1", col1)
    rootTable = rootTable.withColumnRenamed("t3", col2)
    rootTable
  }

  def evaluateTriple(t1: Triple, input: DataFrame): DataFrame = {
    var rootTable = input
    if (t1.getSubject.isVariable && t1.getPredicate.isVariable && t1.getObject.isVariable) {
      if (t1.getSubject.toString().equals(t1.getPredicate.toString()) &&
          t1.getSubject.toString().equals(t1.getObject.toString())) {
        rootTable = rootTable.filter(col("s").equalTo(col("p") &&
                                col("s").equalTo(col("o")))).
                                select(col("s").as(t1.getSubject.toString()))
      } else if (t1.getSubject.toString().equals(t1.getPredicate.toString())) {
        rootTable = rootTable.filter(col("s").equalTo(col("p"))).
                                select(col("s").as(t1.getSubject.toString()), col("o"))
      } else if (t1.getSubject.toString().equals(t1.getObject.toString())) {
        rootTable = rootTable.filter(col("s").equalTo(col("o"))).
                                select(col("s").as(t1.getSubject.toString()), col("p").as(t1.getPredicate.toString()))
      } else if (t1.getPredicate.toString().equals(t1.getObject.toString())) {
        rootTable = rootTable.filter(col("p").equalTo(col("o"))).
                                select(col("s").as(t1.getSubject.toString()), col("p").as(t1.getPredicate.toString()))
      } else {
        rootTable = rootTable.select(col("s").as(t1.getSubject.toString()),
                                col("p").as(t1.getPredicate.toString()),
                                col("o").as(t1.getObject.toString()))
      }
    } else if (t1.getSubject.isVariable && t1.getPredicate.isVariable && !t1.getObject.isVariable) {
      if (t1.getSubject.toString().equals(t1.getPredicate.toString())) {
        rootTable = rootTable.filter(col("o").equalTo(t1.getObject.toString()) &&
                                col("s").equalTo(col("p"))).
                                select(col("s").as(t1.getSubject.toString()))
      } else {
        rootTable = rootTable.filter(col("o").equalTo(t1.getObject.toString())).
                                select(col("s").as(t1.getSubject.toString()), col("p").as(t1.getPredicate.toString()))
      }
    } else if (t1.getSubject.isVariable && !t1.getPredicate.isVariable && t1.getObject.isVariable) {
      if (t1.getSubject.toString().equals(t1.getObject.toString())) {
        rootTable = rootTable.filter(col("p").equalTo(t1.getPredicate.toString()) &&
                                col("s").equalTo(col("o"))).
                                select(col("s").as(t1.getSubject.toString()))
      } else {
        rootTable = rootTable.filter(col("p").equalTo(t1.getPredicate.toString())).
                                select(col("s").as(t1.getSubject.toString()), col("o").as(t1.getObject.toString()))
      }
    } else if (t1.getSubject.isVariable && !t1.getPredicate.isVariable && !t1.getObject.isVariable) {
      rootTable = rootTable.filter(col("p").equalTo(t1.getPredicate.toString()) &&
                                col("o").equalTo(t1.getObject.toString())).
                                select(col("s").as(t1.getSubject.toString()))
    } else if (!t1.getSubject.isVariable && t1.getPredicate.isVariable && t1.getObject.isVariable) {
      if (t1.getPredicate.toString().equals(t1.getObject.toString())) {
        rootTable = rootTable.filter(col("s").equalTo(t1.getSubject.toString()) &&
                                col("p").equalTo(col("o"))).
                                select(col("p").as(t1.getPredicate.toString()))
      } else {
        rootTable = rootTable.filter(col("s").equalTo(t1.getSubject.toString())).
                                select(col("p").as(t1.getPredicate.toString()), col("o").as(t1.getObject.toString()))
      }
    } else if (!t1.getSubject.isVariable && t1.getPredicate.isVariable && !t1.getObject.isVariable) {
      rootTable = rootTable.filter(col("s").equalTo(t1.getSubject.toString()) &&
                                col("o").equalTo(t1.getObject.toString())).
                                select(col("p").as(t1.getPredicate.toString()))
    } else if (!t1.getSubject.isVariable && !t1.getPredicate.isVariable && t1.getObject.isVariable) {
      rootTable = rootTable.filter(col("s").equalTo(t1.getSubject.toString()) &&
                                col("p").equalTo(t1.getPredicate.toString())).
                                select(col("o").as(t1.getObject.toString()))
    } else {
      rootTable = rootTable.filter(col("s").equalTo(t1.getSubject.toString()) &&
                                col("p").equalTo(t1.getPredicate.toString()) &&
                                col("o").equalTo(t1.getObject.toString()))
    }
    rootTable
  }

  def identifyVariables(triple: Triple) : HashSet[String] = {
    val vars = HashSet[String]()

    if(triple.getSubject.isVariable) {
      vars += triple.getSubject.toString()
    }
    if(triple.getPredicate.isVariable) {
      vars += triple.getPredicate.toString()
    }
    if(triple.getObject.isVariable) {
      vars += triple.getObject.toString()
    }
    vars
  }
}
