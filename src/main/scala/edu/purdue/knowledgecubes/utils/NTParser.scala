package edu.purdue.knowledgecubes.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

import edu.purdue.knowledgecubes.rdf.RDFTriple

object NTParser {

  def parse(spark: SparkSession, path: String): DataFrame = {
    val parsed = spark.sparkContext.textFile(path)
      .filter(x => x.split(" ").size > 1)
      .map(line => {
        var value = ""
        if (line.endsWith(".")) {
          value = line.substring(0, line.length - 2)
        }
        var parts = value.split(" ") // Split on space
        var sub = parts(0)
        if (sub.startsWith("<") && sub.endsWith(">")) {
          sub = sub.replace("<", "").replace(">", "")
        }
        var prop = parts(1)
        if (prop.startsWith("<") && prop.endsWith(">")) {
          prop = prop.replace("<", "").replace(">", "")
        }
        var obj = parts.slice(2, parts.size).mkString(" ")
        if (obj.startsWith("<") && obj.endsWith(">")) {
          obj = obj.replace("<", "").replace(">", "")
        }
        if (obj.contains("\"")) {
          obj = obj.replaceAll("\"", "")
        }
        RDFTriple(sub.toInt, prop.toInt, obj.toInt)
      })
    spark.sqlContext.createDataFrame(parsed)
  }

}
