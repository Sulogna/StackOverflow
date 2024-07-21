package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Logger, Level}

import annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Properties.isWin
import scala.io.Source
import scala.io.Codec

object Aliases:
  type Question = Posting
  type Answer = Posting
  type QID = Int
  type HighScore = Int
  type LangIndex = Int
import Aliases.*

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object StackOverflow extends StackOverflow:

  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit =
    val inputFileLocation: String = "/stackoverflow/stackoverflow.csv"
    val resource = getClass.getResourceAsStream(inputFileLocation)
    val inputFile = Source.fromInputStream(resource)(Codec.UTF8)
 
    val lines   = sc.parallelize(inputFile.getLines().toList)
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
//    assert(vectors.count() == 1042132, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)



/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable:

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if arr(2) == "" then None else Some(arr(2).toInt),
              parentId =       if arr(3) == "" then None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if arr.length >= 6 then Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
//  To implement the groupedPostings method , we need to perform the following steps:
//
//  1. Filter the questions and answers separately: Identify questions using postTypeId == 1 and answers using postTypeId == 2.
//  2. Prepare them for a join operation: Extract the QID (question ID) for both questions and answers.
//  3. Perform the join operation: Use a join operation to match answers to their corresponding questions based on the QID.
//  4. Group by QID: Group the resulting pairs by QID to get an RDD[(QID, Iterable[(Question, Answer)])]

  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
  // Filter questions and map to (QID, Question)
  val questions = postings.filter(_.postingType == 1).map(posting => (posting.id, posting))

  // Filter answers and map to (parentId, Answer)
  val answers = postings.filter(_.postingType == 2).map(posting => (posting.parentId.get, posting))

  // Join questions and answers on QID
  val joined = questions.join(answers)

  // Group by QID
  val grouped = joined.groupByKey()

  grouped
}


  /** Compute the maximum score for each posting */
// 1. Takes the grouped RDD and maps each group(QID, Iterable[(Question, Answer)]) to the highest answer score for each question
// 2. The answerHighScore helper function finds the maximum score among an array of answers.
// 3. The mapValues transformation is used to apply the score computation to each group.

  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while i < as.length do
        val score = as(i).score
        if score > highScore then
          highScore = score
        i += 1
      highScore
    }

    grouped.mapValues { iter =>
      val answers = iter.map(_._2).toArray // Extract answers from the Iterable[(Question, Answer)]
      val highestScore = answerHighScore(answers) // Compute the highest score among the answers
      (iter.head._1, highestScore) // Retrieve the first question (since all pairs have the same question) and pair it with the highest score //val question = iter.head._1 //(question, highestScore)
    }.map(_._2) //.map { case (qid, (question, highScore)) => (question, highScore) }
  }


  /** Compute the vectors for the kmeans */

  /*
  1. Filter Out Questions Without Valid Tags: Use the firstLangInTag helper method to determine the language index for each question. If a question doesn't have a valid language tag, it should be ignored.
  2. Create Vectors: For each valid question, create a vector (LangIndex, HighScore) where LangIndex is the index of the language in the langs list multiplied by langSpread.
  3. Return the Resulting RDD: The resulting RDD should contain only the valid (LangIndex, HighScore) pairs. */

  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] =
      tag match
        case None => None
        case Some(lang) =>
          val index = ls.indexOf(lang)
          if (index >= 0) Some(index) else None

    scored.flatMap { case (question, highScore) =>
      // Determine the language index
      val langIndexOpt = firstLangInTag(question.tags, langs)
      // If the language index is valid, create the vector, otherwise ignore
      langIndexOpt match
        case Some(langIndex) => Some((langIndex * langSpread, highScore))
        case None => None
    }
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] =

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] =
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for i <- 0 until size do
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next()

      var i = size.toLong
      while iter.hasNext do
        val elt = iter.next()
        val j = math.abs(rnd.nextLong()) % i
        if j < size then
          res(j.toInt) = elt
        i += 1

      res

    val res =
      if langSpread < 500 then
        // sample the space regardless of the language
        vectors.distinct.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey().flatMap(
          (lang, vectors) => reservoirSampling(lang, vectors.iterator.distinct, perLang).map((lang, _))
        ).collect()

    assert(res.length == kmeansKernels, res.length)
    res


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] =
    // TODO: Compute the groups of points that are the closest to each mean,
    // and then compute the new means of each group of points. Finally, compute
    // a Map that associate the old `means` values to their new values
    //val newMeansMap: scala.collection.Map[(Int, Int), (Int, Int)] = ???

    // Step 1: Assign vectors to the closest mean
    val closest = vectors.map(p => (findClosest(p, means), p))

    // Step 2: Group by the assigned mean and compute the new means
    val newMeansMap = closest.groupByKey().mapValues(averageVectors).collect().toMap
    // Map the new means to the order of the old means
    val newMeans: Array[(Int, Int)] = means.map(oldMean => newMeansMap(oldMean))
    // Step 3: Calculate the distance between old and new means
    val distance = euclideanDistance(means, newMeans)

    if debug then
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for idx <- 0 until kmeansKernels do
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    // Step 4: Check for convergence
    if converged(distance) then
      newMeans
    else if iter < kmeansMaxIterations then
      kmeans(newMeans, vectors, iter + 1, debug)
    else
      if debug then
        println("Reached max iterations!")
      newMeans




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double =
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double =
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while idx < a1.length do
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    sum

  /** Return the center that is the closest to `p` */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): (Int, Int) =
    var bestCenter: (Int, Int) = null
    var closest = Double.PositiveInfinity
    for center <- centers do
      val tempDist = euclideanDistance(p, center)
      if tempDist < closest then
        closest = tempDist
        bestCenter = center
    bestCenter


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) =
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while iter.hasNext do
      val item = iter.next()
      comp1 += item._1
      comp2 += item._2
      count += 1
    ((comp1 / count).toInt, (comp2 / count).toInt)




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey().persist()

    val median = closestGrouped.mapValues { vs =>
      val langCounts = vs.groupBy { case (langIndex, _) => langIndex / langSpread }
        .map { case (langGroup, values) => (langGroup, values.size) }
      val dominantLangIndex = langCounts.maxBy(_._2)._1
      val langLabel: String = langs(dominantLangIndex) // most common language in the cluster
      val langPercent: Double = (langCounts(dominantLangIndex) * 100.0) / vs.size // percent of the questions in the most common language
      val clusterSize: Int = vs.size
      val medianScore: Int = {
        val sortedScores = vs.map(_._2).toList.sorted
        if (sortedScores.size % 2 == 0)
          (sortedScores(sortedScores.size / 2 - 1) + sortedScores(sortedScores.size / 2)) / 2
        else
          sortedScores(sortedScores.size / 2)
      }

        (langLabel, langPercent, clusterSize, medianScore)
      }

      median.collect().map(_._2).sortBy(_._4)
}

      def printResults(results: Array[(String, Double, Int, Int)]): Unit =
        println("Resulting clusters:")
        println("  Score  Dominant language (%percent)  Questions")
        println("================================================")
        for ((lang, percent, size, score) <- results)
          println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")

          /*
          * Partitioning Data:

          Partitioning data can help by distributing the computation across multiple nodes, making the process faster and more efficient, especially with large datasets.
          Persisting Data:

          Persisting data in memory reduces recomputation costs. This can be particularly useful when the data is reused multiple times, as is the case with closestGrouped.
          Java Clusters:

          To determine how many clusters have "Java" as their dominant language, you can filter the results by the language label.
          Standout Java Clusters:

          Standout clusters might have particularly high median scores or a large percentage of Java questions.
          C# vs. Java Clusters:

          Comparing "C#" clusters to "Java" clusters could involve analyzing differences in median scores, cluster sizes, and language percentages.*/

