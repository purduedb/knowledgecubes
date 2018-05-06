package edu.purdue.knowledgecubes.utils


object Timer {

  def timeInSeconds[R](block: => R): Float = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    (t1 - t0) / 1000f
  }

  def timeInMilliseconds[R](block: => R): Long = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    (t1 - t0)
  }
}
