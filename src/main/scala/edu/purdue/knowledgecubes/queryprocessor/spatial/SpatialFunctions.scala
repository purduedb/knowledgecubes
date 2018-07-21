package edu.purdue.knowledgecubes.queryprocessor.spatial



class SpatialFunctions {

  // SpatialExample Selection: queries with a FILTER function with arguments a variable and a constant
  // SpatialExample Join: queries with a FILTER function with two variable arguments

  def contains(latitude_min: String,
               longitude_min: String,
               latitude_max: String,
               longitude_max: String,
               resource_lat: String,
               resource_lon: String): Boolean = {
    true
  }

  def contains(latitude_min: String,
               longitude_min: String,
               latitude_max: String,
               longitude_max: String,
               resource_geom: String): Boolean = {
    true
  }

}
