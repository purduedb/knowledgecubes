package edu.purdue.knowledgecubes.utils


object CliParser {

  case class Config(ntriples: String = "",
                    encoded: String = "",
                    separator: String = "",
                    dataset: String = "",
                    stype: String = "",
                    local: String = "",
                    db: String = "",
                    queries: String = "",
                    fp: String = "",
                    ftype: String = "",
                    spatial: String = "",
                    count: String = "")

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

    opt[String]('f', "fp").required().valueName("rate").
      action((x, c) => c.copy(fp = x)).
      text("False positive rate")

    opt[String]('t', "fType").required().valueName("type").
      action((x, c) => c.copy(ftype = x)).
      text("GEFI type (bloom, roaring, bitset)")

    help("help").text("prints this usage text")
  }

  private val encoderParser = new scopt.OptionParser[Config]("DictionaryEncoderCLI") {
    head("Knowledge Cubes Encoder", "0.1.0")
    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(ntriples = x)).
      text("N-Triples File")

    opt[String]('l', "local").required().valueName("<path>").
      action((x, c) => c.copy(local = x)).
      text("Local directory")

    opt[String]('o', "output").required().valueName("<path>").
      action((x, c) => c.copy(encoded = x)).
      text("Encoded File")

    opt[String]('s', "separator").required().valueName("(tab | space)").
      action((x, c) => c.copy(separator = x)).
      text("Separator (tab | space)")

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

    opt[String]('f', "fp").required().valueName("rate").
      action((x, c) => c.copy(fp = x)).
      text("False positive rate")

    opt[String]('t', "fType").required().valueName("(bloom | roaring | bitset)").
      action((x, c) => c.copy(ftype = x)).
      text("GEFI type (bloom, roaring, bitset)")

    opt[String]('s', "spatial").required().valueName("(y | n)").
      action((x, c) => c.copy(spatial = x)).
      text("Enable spatial support")


    help("help").text("prints this usage text")
  }

  private val joinFilterParser = new scopt.OptionParser[Config]("JoinFiltersCLI") {
    head("Knowledge Cubes Join Filters Creator", "0.1.0")

    opt[String]('l', "local").required().valueName("<path>").
      action((x, c) => c.copy(local = x)).
      text("Local directory")

    opt[String]('d', "db").required().valueName("<path>").
      action((x, c) => c.copy(db = x)).
      text("Database directory")

    opt[String]('f', "fp").required().valueName("rate").
      action((x, c) => c.copy(fp = x)).
      text("False positive rate")

    opt[String]('t', "fType").required().valueName("(bloom | roaring | bitset)").
      action((x, c) => c.copy(ftype = x)).
      text("GEFI type (bloom, roaring, bitset)")

    help("help").text("prints this usage text")
  }

  private val semanticFilterParser = new scopt.OptionParser[Config]("SemanticFiltersCLI") {
    head("Knowledge Cubes Semantic Filters Creator", "0.1.0")

    opt[String]('l', "local").required().valueName("<path>").
      action((x, c) => c.copy(local = x)).
      text("Local directory")

    opt[String]('t', "fType").required().valueName("<path>").
      action((x, c) => c.copy(ftype = x)).
      text("GEFI type (bloom, roaring, bitset)")

    opt[String]('f', "fp").required().valueName("<path>").
      action((x, c) => c.copy(fp = x)).
      text("False positive rate")

    opt[String]('s', "sType").required().valueName("(spatial | temporal | ontological)").
      action((x, c) => c.copy(stype = x)).
      text("Filter Type (spatial, temporal, ontological)")

    opt[String]('r', "dataset").required().valueName("<path>").
      action((x, c) => c.copy(dataset = x)).
      text("RDF Dataset (currently supported: yago, lgd)")

    opt[String]('d', "db").required().valueName("<path>").
      action((x, c) => c.copy(db = x)).
      text("Database directory")

    opt[String]('c', "count").required().valueName("<value>").
      action((x, c) => c.copy(count = x)).
      text("Maximum number of elements per filter")

    help("help").text("prints this usage text")
  }

  def parseLoader(args: Array[String]): Map[String, String] = {
    var parameters = Map[String, String]()
    loaderParser.parse(args, Config()) match {
      case Some(config) =>
        parameters += ("ntriples" -> config.ntriples)
        parameters += ("local" -> config.local)
        parameters += ("db" -> config.db)
        parameters += ("fType" -> config.ftype)
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
        parameters += ("fType" -> config.ftype)
        parameters += ("fp" -> config.fp)
        parameters += ("spatial" -> config.spatial)
      case None =>
        println(executorParser.usage)
        System.exit(1)
    }
    parameters
  }

  def parseGEFIFiltering(args: Array[String]): Map[String, String] = {
    var parameters = Map[String, String]()
    joinFilterParser.parse(args, Config()) match {
      case Some(config) =>
        parameters += ("local" -> config.local)
        parameters += ("db" -> config.db)
        parameters += ("fType" -> config.ftype)
        parameters += ("fp" -> config.fp)
      case None =>
        println(joinFilterParser.usage)
        System.exit(1)
    }
    parameters
  }

  def parseSemanticFiltering(args: Array[String]): Map[String, String] = {
    var parameters = Map[String, String]()
    semanticFilterParser.parse(args, Config()) match {
      case Some(config) =>
        parameters += ("local" -> config.local)
        parameters += ("fType" -> config.ftype)
        parameters += ("fp" -> config.fp)
        parameters += ("sType" -> config.stype)
        parameters += ("input" -> config.ntriples)
        parameters += ("dataset" -> config.dataset)
        parameters += ("db" -> config.db)
        parameters += ("count" -> config.count)
      case None =>
        println(semanticFilterParser.usage)
        System.exit(1)
    }
    parameters
  }

  def parseEncoder(args: Array[String]): Map[String, String] = {
    var parameters = Map[String, String]()
    encoderParser.parse(args, Config()) match {
      case Some(config) =>
        parameters += ("local" -> config.local)
        parameters += ("input" -> config.ntriples)
        parameters += ("output" -> config.encoded)
        parameters += ("separator" -> config.separator)
      case None =>
        println(encoderParser.usage)
        System.exit(1)
    }
    parameters
  }
}
