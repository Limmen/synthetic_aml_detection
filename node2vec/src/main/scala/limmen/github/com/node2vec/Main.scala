package limmen.github.com.node2vec

import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
import org.rogach.scallop.ScallopConf
import limmen.github.com.node2vec._
import org.apache.spark.graphx._

/**
 * Parser of command-line arguments
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val dim = opt[Int](required = true, validate = (0<), descr = "dimension of embeddings [100]", default = Some(100))
  val lr = opt[Double](required = true, validate = (0<), default = Some(0.025), descr = "learning rate [0.025]")
  val iterations = opt[Int](required = true, validate = (0<), default = Some(10), descr = "number of iterations for training [10]")
  val partitions = opt[Int](required = true, validate = (0<), default = Some(10), descr = "number of partitions to distribute training on [10]")
  val windowsize = opt[Int](required = true, validate = (0<), default = Some(5), descr = "Context window size for training [5]")
  val walklength = opt[Int](required = true, validate = (0<), descr = "walk length [80]", default = Some(80))
  val numwalks = opt[Int](required = true, validate = (0<), descr = "number of walks [10]", default = Some(10))
  val p = opt[Double](required = true, validate = (0<), default = Some(1.0), descr = "p [1.0]")
  val q = opt[Double](required = true, validate = (0<), default = Some(1.0), descr = "q [1.0]")
  val weighted = opt[Boolean](descr = "weighted")
  val directed = opt[Boolean](descr = "directed")
  val degree = opt[Int](required = true, validate = (0<), descr = "degree [30]", default = Some(30))
  val indexed = opt[Boolean](descr = "indexed")
  val nodepath = opt[String](required = false, descr = "nodePath", default = Some(null))
  val input = opt[String](required = true, descr = "input path")
  val cluster = opt[Boolean](descr = "Flag set to true means that the application is running in cluster mode, otherwise it runs locally")
  val output = opt[String](required = true, descr = "output folder path")
  val cmd = opt[String](required = true, descr = "command")
  verify()
}

object Command extends Enumeration {
  type Command = Value
  val node2vec, randomwalk, embedding = Value
}

object Main {
  import Command._
  case class Params(
    iter: Int = 10,
    lr: Double = 0.025,
    numPartition: Int = 10,
    dim: Int = 128,
    window: Int = 10,
    walkLength: Int = 80,
    numWalks: Int = 10,
    p: Double = 1.0,
    q: Double = 1.0,
    weighted: Boolean = true,
    directed: Boolean = false,
    degree: Int = 30,
    indexed: Boolean = true,
    nodePath: String = null,
    input: String = null,
    output: String = null,
    cmd: Command = null)

  def main(args: Array[String]): Unit = {

    // Setup logging
    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)
    log.info(s"Starting Node2vec")

    //Parse cmd arguments
    val conf = new Conf(args)
    var cmd: Command.Value = null
    conf.cmd() match {
      case "embedding" => cmd = Command.embedding
      case "randomwalk" => cmd = Command.randomwalk
      case "node2vec" => cmd = Command.node2vec
    }
    val params = Params(iter = conf.iterations(), lr = conf.lr(), numPartition = conf.partitions(), dim = conf.dim(), window = conf.windowsize(),
      walkLength = conf.walklength(), numWalks = conf.numwalks(), p = conf.p(), q = conf.q(), weighted = conf.weighted(), directed = conf.directed(),
      degree = conf.degree(), indexed = conf.indexed(), nodePath = conf.nodepath(), input = conf.input(), output = conf.output(), cmd = cmd)

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

    Node2vec.setup(sc, params)
    params.cmd match {
      case Command.node2vec => Node2vec.load()
        .initTransitionProb()
        .randomWalk()
        .embedding()
        .save()
      case Command.randomwalk => Node2vec.load()
        .initTransitionProb()
        .randomWalk()
        .saveRandomPath()
      case Command.embedding => {
        val randomPaths = Word2vec.setup(sc, params).read(params.input)
        Word2vec.fit(randomPaths).save(params.output)
        Node2vec.loadNode2Id(params.nodePath).saveVectors()
      }
    }

    import spark.implicits._

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
    new SparkConf().setAppName("AMLGraph").set("spark.executor.heartbeatInterval", "20s").set("spark.rpc.message.maxSize", "512").set("spark.kryoserializer.buffer.max", "1024").set("spark.hadoop.validateOutputSpecs", "false")
  }

  /**
   * Utility function for printing training configuration
   *
   * @param conf command line arguments
   * @param log logger
   * @return configuration string
   */
  def printArgs(conf: Conf, log: Logger): String = {
    val argsStr = s"Args:  | input: ${conf.input()} | output: ${conf.output()} | cluster: ${conf.cluster()} | partitions: ${conf.partitions()} | dim: ${conf.dim()} | lr: ${conf.lr()} | iterations: ${conf.iterations()} | windowsize: ${conf.windowsize()} | walklength: ${conf.walklength()} | numwalks: ${conf.walklength()} | p: ${conf.p()} | q: ${conf.q()} | weighted: ${conf.weighted()} | directed: ${conf.directed()} | degree: ${conf.degree()} | indexed: ${conf.indexed()} | nodepath: ${conf.nodepath()} | cmd: ${conf.cmd()}"
    log.info(argsStr)
    argsStr
  }
}
