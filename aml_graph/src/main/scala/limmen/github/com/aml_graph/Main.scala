package limmen.github.com.aml_graph

import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.rogach.scallop.ScallopConf
import limmen.github.com.aml_graph._

/**
 * Parser of command-line arguments
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = false, descr = "input path")
  val cluster = opt[Boolean](descr = "Flag set to true means that the application is running in cluster mode, otherwise it runs locally")
  val output = opt[String](required = false, descr = "output folder path")
  verify()
}

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

    val sc = new SparkContext(sparkConf)

    val clusterStr = sc.getConf.toDebugString
    log.info(s"Cluster settings: \n" + clusterStr)

    //Read input
    //val input = sc.textFile(conf.input()).map(line => line.split(" ").toSeq)
  }

  /**
   * Hard coded settings for local spark training
   *
   * @return spark configuration
   */
  def localSparkSetup(): SparkConf = {
    new SparkConf().setAppName("FastTextOnSpark").setMaster("local[*]")
  }

  /**
   * Hard coded settings for cluster spark training
   *
   * @return spark configuration
   */
  def sparkClusterSetup(): SparkConf = {
    new SparkConf().setAppName("FastTextOnSpark").set("spark.executor.heartbeatInterval", "20s").set("spark.rpc.message.maxSize", "512").set("spark.kryoserializer.buffer.max", "1024")
  }

  /**
   * Utility function for printing training configuration
   *
   * @param conf command line arguments
   * @param log logger
   * @return configuration string
   */
  def printArgs(conf: Conf, log: Logger): String = {
    val argsStr = s"Args:  | input: ${conf.input()} | output: ${conf.output()} | cluster: ${conf.cluster()}"
    log.info(argsStr)
    argsStr
  }
}
