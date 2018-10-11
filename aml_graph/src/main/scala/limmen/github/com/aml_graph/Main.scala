package limmen.github.com.aml_graph

import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
import org.rogach.scallop.ScallopConf
import limmen.github.com.aml_graph._
import org.apache.spark.graphx._

/**
 * Parser of command-line arguments
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = false, descr = "input path")
  val cluster = opt[Boolean](descr = "Flag set to true means that the application is running in cluster mode, otherwise it runs locally")
  val output = opt[String](required = false, descr = "output folder path")
  val outputmetadata = opt[String](required = false, descr = "output metadata folder path")
  val partitions = opt[Int](required = true, validate = (0<), default = Some(1), descr = "number of partitions to distribute the graph")
  verify()
}

case class CustomerNode(id: Long)
case class TransactionEdge(
  isFraud: Int,
  transactionType: Int,
  step: Double,
  amount: Double,
  oldBalanceOrg: Double,
  newBalanceOrg: Double,
  oldBalanceDest: Double,
  newBalanceDest: Double)
case class Transaction(
  transactionType: Int,
  isFraud: Int,
  nameOrig: Long,
  nameDest: Long,
  step: Double,
  amount: Double,
  oldBalanceOrg: Double,
  newBalanceOrg: Double,
  oldBalanceDest: Double,
  newBalanceDest: Double)

object Main {

  def main(args: Array[String]): Unit = {

    // Setup logging
    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)
    log.info(s"Starting AMLGraph")

    //Parse cmd arguments
    val conf = new Conf(args)

    //Save the configuration string
    val argsStr = printArgs(conf, log)

    // Setup Spark
    var sparkConf: SparkConf = null
    if (conf.cluster()) {
      sparkConf = sparkClusterSetup()
    } else {
      sparkConf = localSparkSetup()
    }

    val spark = SparkSession.builder().config(sparkConf).getOrCreate();

    val sc = spark.sparkContext

    val clusterStr = sc.getConf.toDebugString
    log.info(s"Cluster settings: \n" + clusterStr)

    import spark.implicits._

    //Read input
    log.info("Reading input CSV file")
    val rawDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(conf.input()).repartition(conf.partitions())
    val typedRdd = rawDf.rdd.map((row: Row) => Transaction(transactionType = row.getInt(0), isFraud = row.getInt(1), nameOrig = row.getInt(2).toLong, nameDest = row.getInt(3).toLong, step = row.getDouble(4), amount = row.getDouble(5), oldBalanceOrg = row.getDouble(6), newBalanceOrg = row.getDouble(7), oldBalanceDest = row.getDouble(8), newBalanceDest = row.getDouble(9)))
    val rawDs = typedRdd.toDS()
    log.info("Extracting node list")
    val origNodes = rawDs.map((transaction: Transaction) => transaction.nameOrig)
    val destNodes = rawDs.map((transaction: Transaction) => transaction.nameDest)
    val nodes: RDD[(VertexId, CustomerNode)] = origNodes.union(destNodes).distinct.map((nodeId: Long) => (nodeId, CustomerNode(nodeId))).rdd
    log.info("Counting nodes")
    val numberOfNodes = nodes.count()
    log.info(s"Number of unique nodes: ${numberOfNodes}")
    log.info("Extracting edge list")
    val edges = rawDs.map((transaction: Transaction) => Edge(transaction.nameOrig, transaction.nameDest,
      TransactionEdge(
        isFraud = transaction.isFraud, transactionType = transaction.transactionType, step = transaction.step,
        amount = transaction.amount, oldBalanceOrg = transaction.oldBalanceOrg, newBalanceOrg = transaction.newBalanceOrg,
        oldBalanceDest = transaction.oldBalanceDest, newBalanceDest = transaction.newBalanceDest))).rdd
    val edgeListDF = rawDs.map((transaction: Transaction) => (transaction.nameOrig.toInt, transaction.nameDest.toInt, transaction.amount))
    val edgeListPath = conf.output() + "/edges"
    log.info(s"Writing weighted edge list in format (from to amount) to: ${edgeListPath}")
    edgeListDF.write.mode("overwrite").option("sep", " ").option("header", "false").csv(edgeListPath)
    log.info("Counting edges")
    val numberOfEdges = edges.count()
    log.info(s"Number of unique edges: ${numberOfEdges}")
    log.info("Creating graph from node and edge list")
    val graph = Graph(nodes, edges)
    log.info("Graph Created")
    log.info("Computing pageranks")
    val alpha = 0.0001
    val pageRanks = graph.pageRank(alpha).vertices
    val pageRanksPath = conf.outputmetadata() + "/pageranks"
    log.info(s"Writing pageranks in format (node PR) to: ${pageRanksPath}")
    pageRanks.toDF.write.mode("overwrite").option("sep", " ").option("header", "false").csv(pageRanksPath)
    log.info("Computing connected components")
    val connectedComponents = graph.connectedComponents()
    val connectedComponentsPath = conf.outputmetadata() + "/connectedcomponents"
    log.info(s"Writing connected components in format (node, idOfComponent) to: ${connectedComponentsPath}")
    connectedComponents.vertices.toDF.write.mode("overwrite").option("sep", " ").option("header", "false").csv(connectedComponentsPath)
    log.info("Counting the number of connected components")
    val numberOfConnectedComponents = connectedComponents.vertices.map { case (_, cc) => cc }.distinct.count()
    log.info(s"Number of connected components: ${numberOfConnectedComponents}")
    log.info("Extracting the triangles per node in the graph")
    val triangleCount = graph.triangleCount()
    log.info("Counting number of triangles in total")
    val numberOfTriangles = triangleCount.vertices.toDF.rdd.map((row: Row) => row.getInt(1)).sum / numberOfNodes
    log.info(s"Number of triangles: ${numberOfTriangles}")
    val trianglesOutputPath = conf.outputmetadata() + "/trianglecounts"
    log.info(s"Writing triangle counts in format (node triangleCount) to path: ${trianglesOutputPath}")
    triangleCount.vertices.toDF.write.mode("overwrite").option("sep", " ").option("header", "false").csv(trianglesOutputPath)
    val metadata = sc.parallelize(Seq((numberOfNodes, numberOfEdges, numberOfTriangles, numberOfConnectedComponents))).toDF(
      "numberOfNodes", "numberOfEdges", "numberOfTriangles", "numberOfConnectedComponents")
    val metadataOutputPath = conf.outputmetadata() + "/stats"
    log.info(s"Writing metadata in format (numberOfNodes,numberOfEdges,numberOfTriangles,numberOfConnectedComponents) to path: ${metadataOutputPath}")
    metadata.coalesce(1).write.mode("overwrite").option("sep", ",").option("header", "true").csv(metadataOutputPath)
    log.info("Shutting down spark job")
    spark.close
  }

  /**
   * Hard coded settings for local spark training
   *
   * @return spark configuration
   */
  def localSparkSetup(): SparkConf = {
    new SparkConf().setAppName("AMLGraph").setMaster("local[*]")
  }

  /**
   * Hard coded settings for cluster spark training
   *
   * @return spark configuration
   */
  def sparkClusterSetup(): SparkConf = {
    new SparkConf().setAppName("AMLGraph").set("spark.executor.heartbeatInterval", "20s").set("spark.rpc.message.maxSize", "512").set("spark.kryoserializer.buffer.max", "1024")
  }

  /**
   * Utility function for printing training configuration
   *
   * @param conf command line arguments
   * @param log logger
   * @return configuration string
   */
  def printArgs(conf: Conf, log: Logger): String = {
    val argsStr = s"Args:  | input: ${conf.input()} | output: ${conf.output()} | cluster: ${conf.cluster()} | partitions: ${conf.partitions()}"
    log.info(argsStr)
    argsStr
  }
}
