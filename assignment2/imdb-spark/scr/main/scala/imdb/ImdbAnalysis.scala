package imdb

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]]) {
  def getGenres(): List[String] = genres.getOrElse(List[String]())
}
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  val conf: SparkConf = new SparkConf().setAppName("hw2").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath).map(ImdbData.parseTitleBasics)

  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath).map(ImdbData.parseTitleRatings)

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath).map(ImdbData.parseTitleCrew)

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath).map(ImdbData.parseNameBasics)

  //add more TitleBasics instances when more than one genres for a title
  def seperate_line_instance(instance: TitleBasics): TraversableOnce[TitleBasics] = instance.genres match {
    case Some(l) => l.map(g => TitleBasics(
      instance.tconst, instance.titleType, instance.primaryTitle, instance.originalTitle, instance.isAdult, 
      instance.startYear, instance.endYear, instance.runtimeMinutes, Some(List(g))))
    case None => List(instance)
  }

  //add more nameBasics instances when more than one knownForTitles
  def seperate_line_instance2(instance: NameBasics): TraversableOnce[NameBasics] = instance.knownForTitles match {
    case Some(l) => l.map(t => NameBasics(
      instance.nconst, instance.primaryName, instance.birthYear, instance.deathYear, 
      instance.primaryProfession, Some(List(t))))
    case None => List(instance)
  }

  //convert type of runtimeMinutes, startYear
  def change_type_int(x: Option[Int]): Int = x match {
    case Some(i) => i 
    case None => -1
  }

  //convert type of genres, knownForTitles to String
  def change_type_string(x: Option[List[String]]): String = x match {
    case Some(i) => i.head
    case None => "None"
  }

  //convert type of primaryTitle, primaryName to String
  def change_type_string2(x: Option[String]): String = x match {
      case Some(s) => s
      case None => "None" //which is not possible to reach as None type has already filtered 
  }

  //get the decade based on its startYear
  def get_decade(year: Int): Int = {
    (year - 1900) / 10
  }

  //continuously compare the ratings and return tconst with higher rating
  def find_max_rating(tuple1: (String, String, Float), tuple2: (String, String, Float)): (String, String, Float) = {
    if (tuple1._3 > tuple2._3) {tuple1} 
    else if (tuple1._3 == tuple2._3) {
      //compare primaryTitle string, select the one comes first alphabetically
      if (tuple1._2 < tuple2._2) tuple1 else tuple2
    } else {tuple2}
  }

  //set counts for films: 1, if they satisfy the requirement; 0, if not
  def film_count(pair: ((String, String), Option[Int])): ((String, String), Int) = {
      if (pair._2 == None) {
        (pair._1, 0)
      } else {
          if (change_type_int(pair._2) >= 2010 && 
                change_type_int(pair._2) <= 2021) (pair._1, 1) else (pair._1, 0)
      }
  }
  
  def task1(rdd: RDD[TitleBasics]): RDD[(Float, Int, Int, String)] = {
    //remove rows with None values for runtimeMinutes column
    val re_rdd = rdd.filter(e => e.runtimeMinutes != None && e.genres != None)
    val extend_rdd = re_rdd.flatMap(seperate_line_instance)
    //only take intrested columns into pairs (genre, runtimeMinutes)
    val pair_rdd = extend_rdd.map(x => (change_type_string(x.genres), change_type_int(x.runtimeMinutes)))
    //group by their genres, and reduce to calculate avg time
    // val grouped: RDD[(String, Iterable[Int])] = pair_rdd.groupByKey()
    val avg_time = pair_rdd.groupByKey().map {case (k, v) => k -> (v.sum / v.size.toFloat)}
    // val avg_time = pair_rdd.mapValues(time => (time, 1)).reduceByKey {case (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)}
    //                                                     .mapValues {case (sum, cnt) => sum / cnt}
    val max_time = pair_rdd.reduceByKey((v1, v2) => if (v1 > v2) v1 else v2)
    val min_time = pair_rdd.reduceByKey((v1, v2) => if (v1 < v2) v1 else v2)
    //the structure after applying two join functions: (genre, ((avg, min), max))
    avg_time.join(min_time).join(max_time).map(x => (x._2._1._1, x._2._1._2, x._2._2, x._1))
  }

  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[String] = {
    //l1: remove rows with None values for startYear column,
    //and select rows with "movie" for titleType
    val re_l1 = l1.filter(e => e.startYear != None && e.titleType == Some("movie") && e.primaryTitle != None)
    val pair_l1 = re_l1.map(x => (x.tconst, (change_type_string2(x.primaryTitle), change_type_int(x.startYear))))
    val filtered_l1 = pair_l1.filter(t => t._2._2 >= 1990 && t._2._2 <= 2018)
    //select from l2 satisfying the requirement and make pair pdd
    val filtered_l2 = l2.filter(x => x.averageRating >= 7.5 && x.numVotes >= 500000).map(x => (x.tconst, 
                                                                                (x.averageRating, x.numVotes)))
    //join two rdd
    val joined = filtered_l1.join(filtered_l2)
    joined.map(x => x._2._1._1)
  }

  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, String, String)] = {
    //l1: remove rows with None values for startYear column
    val re_l1 = l1.filter(e => e.primaryTitle != None && e.genres != None &&
                             change_type_int(e.startYear) >= 1900 && change_type_int(e.startYear) <= 1999 && 
                             e.titleType == Some("movie"))
    //deal with more than one genre for an object
    val extend_l1 = re_l1.flatMap(seperate_line_instance)
    //only contain intrested columns of l1 into tuples
    val l1_pair = extend_l1.map(x => (x.tconst, (get_decade(change_type_int(x.startYear)), 
                                    change_type_string(x.genres), change_type_string2(x.primaryTitle))))
    val l2_pair = l2.map(x => (x.tconst, x.averageRating))
    //after join: (tconst, ((decade, genre, title), rating))
    val joined = l1_pair.join(l2_pair)
    //construct pair rdd with composite keys: ((decade, genre), (tconst, title, rating))
    val joined2 = joined.map(x => ((x._2._1._1, x._2._1._2), (x._1, x._2._1._3, x._2._2)))
    //grouped and return the highest rating tuple as value: RDD[((decade, genre), (tconst, title, rating))]
    val grouped = joined2.reduceByKey((t1, t2) => find_max_rating(t1, t2)).sortByKey()
    //reconstruct results to (decade, genre, title)
    grouped.map(x => (x._1._1, x._1._2, x._2._2))
  }

  // Hint: There could be an input RDD that you do not really need in your implementation.
  def task4(l1: RDD[TitleBasics], l2: RDD[TitleCrew], l3: RDD[NameBasics]): RDD[(String, Int)] = {
    //remove None for intrested Option type in l3
    val l3_f = l3.filter(e => e.primaryName != None && e.knownForTitles != None)
    //extend rdd with one knownForTitle a row
    val extend_rdd3 = l3_f.flatMap(seperate_line_instance2)
    //for l3: construct nested pair rdd (knownForTitles, (nconst, primaryName))
    val pair_rdd3 = extend_rdd3.map(x => (change_type_string(x.knownForTitles), (x.nconst, change_type_string2(x.primaryName))))
    //for l1: construct pair rdd (tconst, startYear)
    val pair_rdd1 = l1.map(x => (x.tconst, change_type_int(x.startYear)))
    //join two rdds (note: if rdd1 doesn't have the value, return None; otherwise, return Some())
    val left_joint = pair_rdd3.leftOuterJoin(pair_rdd1)
    //construct new rdd, and count the number of films satisfying the selection requirement 
    //((nconst, primaryName), count)
    val map_film = left_joint.map(x => (x._2._1, x._2._2)).map(p => film_count(p))
    val film_cnt = map_film.reduceByKey(_ + _)
    //filter the results with at least two counts
    val filtered = film_cnt.filter(_._2 >= 2)
    filtered map {case (k, cnt) => (k._2, cnt)}
  }

  def main(args: Array[String]) = {
    val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val topRated = timed("Task 3", task3(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val crews = timed("Task 4", task4(titleBasicsRDD, titleCrewRDD, nameBasicsRDD).collect().toList)
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
