package edu.purdue.knowledgecubes.metadata

import org.apache.spark.sql.{Column, DataFrame}

class Result(val output: DataFrame,
             val projectionList: List[Column],
             val numResults: Long,
             val tableSizes: Long,
             val reductionSizes: Long,
             val maxJoins: Int,
             val isWarm: Boolean,
             val numTriples: Int,
             var execTime: Double,
             var loadTime: Long,
             var filterTime: Long) {
}
