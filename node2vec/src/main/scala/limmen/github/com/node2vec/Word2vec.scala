package limmen.github.com.node2vec

import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.SparkContext
//import org.apache.spark.mllib.feature.{ Word2Vec, Word2VecModel }
import org.apache.spark.rdd.RDD
import limmen.github.com.node2vec._
object Word2vec extends Serializable {
  var context: SparkContext = null
  var word2vec = new Word2Vec()
  var vectors: Map[String, Array[Float]] = null
  //var model: Word2VecModel = null

  def setup(context: SparkContext, param: Main.Params): this.type = {
    LogHolderW2V.log.info("Setup")
    println("Setup")
    this.context = context
    /**
     * model = sg
     * update = hs
     */
    word2vec.setLearningRate(param.lr)
      .setNumIterations(param.iter)
      .setNumPartitions(param.numPartition)
      .setMinCount(0)
      .setVectorSize(param.dim)
      .setVerbose(true)
      .setWindowSize(param.window)

    //val word2vecWindowField = word2vec.getClass.getDeclaredField("org$apache$spark$mllib$feature$Word2Vec$$window")
    //word2vecWindowField.setAccessible(true)
    //word2vecWindowField.setInt(word2vec, param.window)

    this
  }

  def read(path: String): RDD[Iterable[String]] = {
    LogHolderW2V.log.info("reading edges")
    println("reading edges")
    context.textFile(path).repartition(200).map(_.split("\\s").toSeq)
  }

  def fit(input: RDD[Iterable[String]], sc: SparkContext, outputPath: String): this.type = {
    LogHolderW2V.log.info("word2vec training on inputRDD")
    println("word2vec training on inputRDD")
    val model = word2vec.train(input)
    vectors = model.wordVectors
    model.saveToWord2VecFormat(model.wordVectors, outputPath, sc, false)
    this
  }

  def save(outputPath: String): this.type = {
    //model.save(context, s"$outputPath.bin")
    this
  }

  def load(path: String): this.type = {
    //model = Word2VecModel.load(context, path)

    this
  }

  def getVectors = vectors

  /**
   * Utility to make sure logger is serializable
   */
  object LogHolderW2V extends Serializable {
    @transient lazy val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)
  }
}
