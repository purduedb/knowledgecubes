package edu.purdue.knowledgecubes.queryprocessor.spatial



class SpatialFunctions {

  // SpatialExample Selection: queries with a FILTER function with arguments a variable and a constant
  // SpatialExample Join: queries with a FILTER function with two variable arguments

  def within(variable_lat: String,
             variable_lon: String,
             latitude_min: String,
             longitude_min: String,
             latitude_max: String,
             longitude_max: String): Boolean = {
    true
  }

  def within(variable_geom: String,
             latitude_min: String,
             longitude_min: String,
             latitude_max: String,
             longitude_max: String): Boolean = {
    true
  }

}
