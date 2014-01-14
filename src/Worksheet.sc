import org.apache.spark.SparkContext
import scala.io._
import scala.annotation.tailrec
import scala.collection.breakOut
import scala.collection.generic.CanBuildFrom

object Worksheet {

  // mode computation function from http://rosettacode.org/wiki/Average/Mode#Scala
  def mode[T, CC[X] <: Seq[X]](coll: CC[T])(implicit o: T => Ordered[T], cbf: CanBuildFrom[Nothing, T, CC[T]]): CC[T] = {
    val grouped = coll.groupBy(x => x).mapValues(_.size).toSeq
    val max = grouped.map(_._2).max
    grouped.filter(_._2 == max).map(_._1)(breakOut)
  }                                               //> mode: [T, CC[X] <: Seq[X]](coll: CC[T])(implicit o: T => Ordered[T], implici
                                                  //| t cbf: scala.collection.generic.CanBuildFrom[Nothing,T,CC[T]])CC[T]

  def countSubstring(str1: String, str2: String): Int = {
    @tailrec def count(pos: Int, c: Int): Int = {
      val idx = str1 indexOf (str2, pos)
      if (idx == -1) c else count(idx + 1, c + 1)
    }
    count(0, 0)
  }                                               //> countSubstring: (str1: String, str2: String)Int

  def sum(list: List[Int]): Int = list.foldLeft(0)((acc, x) => acc + x)
                                                  //> sum: (list: List[Int])Int

  val databaseFasta = Source.fromFile("/home/binilbenjamin/workspace/spark-kmer-mode/src/db.fa")
                                                  //> databaseFasta  : scala.io.BufferedSource = non-empty iterator
  val database = databaseFasta.getLines.toSeq     //> database  : Seq[String] = Stream(actgaactgaactgagctagctagctagcta, ?)

  val query = "atgagctaactgagctacacta"            //> query  : java.lang.String = atgagctaactgagctacacta

  val kmerSize = 5                                //> kmerSize  : Int = 5

  val sc = new SparkContext("local[2]", "Kmer mode computation")
                                                  //> sc  : org.apache.spark.SparkContext = org.apache.spark.SparkContext@aca62b1
                                                  //| 

  val dbRDD = sc.parallelize(database)            //> dbRDD  : org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at par
                                                  //| allelize at Worksheet.scala:35

  val resultRDD = dbRDD.map(record =>
    {
      lazy val kmerList = query sliding (kmerSize) toList

      kmerList.map(kmer => { (query, countSubstring(record, kmer)) });

    })                                            //> resultRDD  : org.apache.spark.rdd.RDD[List[(java.lang.String, Int)]] = Mapp
                                                  //| edRDD[1] at map at Worksheet.scala:37

  println(mode(resultRDD.collect.toList.map(_.map(_._2)).transpose.map(_.sum).filter(_ > 1)))
                                                  //> List(5)

}