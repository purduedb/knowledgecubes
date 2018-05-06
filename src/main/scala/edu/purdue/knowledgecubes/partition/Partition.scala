package edu.purdue.knowledgecubes.partition

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col

import edu.purdue.knowledgecubes.rdf.RDFTriple

object Partition {

  def byDefaultCriteria(dataFrame: Dataset[RDFTriple]): Dataset[RDFTriple] = dataFrame.repartition(col("s"))

  def byJoinAttribute(joinAttribute: String,
                      dataFrame: Dataset[RDFTriple]): Dataset[RDFTriple] = dataFrame.repartition(col("s"))

}
