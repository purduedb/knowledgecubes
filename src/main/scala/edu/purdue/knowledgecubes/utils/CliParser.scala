package edu.purdue.knowledgecubes.utils


object CliParser {

  case class Config(ntriples: String = "",
                    local: String = "",
                    db: String = "",
                    queries: String = "",
                    fp: String = "",
                    ftype: String = "")

  private val loaderParser = new scopt.OptionParser[Config]("StoreCLI") {
    head("Knowledge Cubes Store Creator", "0.1.0")
    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(ntriples = x)).
      text("N-Triples File")

    opt[String]('l', "local").required().valueName("<path>").
      action((x, c) => c.copy(local = x)).
      text("Local directory")

    opt[String]('d', "db").required().valueName("<path>").
      action((x, c) => c.copy(db = x)).
      text("Database directory")

    opt[String]('f', "fp").required().valueName("<path>").
      action((x, c) => c.copy(fp = x)).
      text("False positive rate")

    opt[String]('t', "ftype").required().valueName("<path>").
      action((x, c) => c.copy(ftype = x)).
      text("GEFI type (bloom,cuckoo,roaring,bitset)")

    help("help").text("prints this usage text")
  }

  private val executorParser = new scopt.OptionParser[Config]("QueryCLI") {
    head("Knowledge Cubes Executor", "0.1.0")

    opt[String]('l', "local").required().valueName("<path>").
      action((x, c) => c.copy(local = x)).
      text("Local directory")

    opt[String]('d', "db").required().valueName("<path>").
      action((x, c) => c.copy(db = x)).
      text("Database directory")

    opt[String]('q', "Query directory").required().valueName("<path>").
      action((x, c) => c.copy(queries = x)).
      text("Query Directory")

    opt[String]('f', "fp").required().valueName("<path>").
      action((x, c) => c.copy(fp = x)).
      text("False positive rate")

    opt[String]('t', "ftype").required().valueName("<path>").
      action((x, c) => c.copy(ftype = x)).
      text("GEFI type (bloom,cuckoo,roaring,bitset)")

    help("help").text("prints this usage text")
  }

  private val filterParser = new scopt.OptionParser[Config]("FilterCLI") {
    head("Knowledge Cubes GEFI Creator", "0.1.0")

    opt[String]('l', "local").required().valueName("<path>").
      action((x, c) => c.copy(local = x)).
      text("Local directory")

    opt[String]('d', "db").required().valueName("<path>").
      action((x, c) => c.copy(db = x)).
      text("Database directory")

    opt[String]('f', "fp").required().valueName("<path>").
      action((x, c) => c.copy(fp = x)).
      text("False positive rate")

    opt[String]('t', "ftype").required().valueName("<path>").
      action((x, c) => c.copy(ftype = x)).
      text("GEFI type (bloom,cuckoo,roaring,bitset)")

    help("help").text("prints this usage text")
  }


  def parseLoader(args: Array[String]): Map[String, String] = {
    var parameters = Map[String, String]()
    loaderParser.parse(args, Config()) match {
      case Some(config) =>
        parameters += ("ntriples" -> config.ntriples)
        parameters += ("local" -> config.local)
        parameters += ("db" -> config.db)
        parameters += ("ftype" -> config.ftype)
        parameters += ("fp" -> config.fp)
      case None =>
        println(loaderParser.usage)
        System.exit(1)
    }
    parameters
  }

  def parseExecutor(args: Array[String]): Map[String, String] = {
    var parameters = Map[String, String]()
    executorParser.parse(args, Config()) match {
      case Some(config) =>
        parameters += ("local" -> config.local)
        parameters += ("db" -> config.db)
        parameters += ("queries" -> config.queries)
        parameters += ("ftype" -> config.ftype)
        parameters += ("fp" -> config.fp)
      case None =>
        println(executorParser.usage)
        System.exit(1)
    }
    parameters
  }

  def parseFilter(args: Array[String]): Map[String, String] = {
    var parameters = Map[String, String]()
    filterParser.parse(args, Config()) match {
      case Some(config) =>
        parameters += ("local" -> config.local)
        parameters += ("db" -> config.db)
        parameters += ("ftype" -> config.ftype)
        parameters += ("fp" -> config.fp)
      case None =>
        println(filterParser.usage)
        System.exit(1)
    }
    parameters
  }
}
