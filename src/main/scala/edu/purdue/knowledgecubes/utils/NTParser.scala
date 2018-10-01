package edu.purdue.knowledgecubes.utils

import org.apache.spark.sql.{Dataset, SparkSession}

import edu.purdue.knowledgecubes.rdf.RDFTriple

object NTParser {

  def parse(spark: SparkSession, path: String): Dataset[RDFTriple] = {
    import spark.sqlContext.implicits._
    spark.read.textFile(path)
      .map(line => {
        var parts = line.split(" ") // Split on space
        var sub = parts(0)
        var prop = parts(1)
        var obj = parts(2)
        RDFTriple(sub.toInt, prop.toInt, obj.toInt)
      })
  }
}
