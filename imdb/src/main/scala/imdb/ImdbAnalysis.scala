package imdb
import scala.io.Source


case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsList: List[TitleBasics] = Source.fromFile(ImdbData.titleBasicsPath).getLines.toList.map(ImdbData.parseTitleBasics)
  // val titleBasicsList: List[TitleBasics] = Source.fromFile(ImdbData.titleBasicsPath).getLines.toList.foldRight(
  //   Nil:List[TitleBasics])((line, acc) => ImdbData.parseTitleBasics(line) :: acc)
  
  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsList: List[TitleRatings] = Source.fromFile(ImdbData.titleRatingsPath).getLines.toList.map(ImdbData.parseTitleRatings)
  // val titleRatingsList: List[TitleRatings] = Source.fromFile(ImdbData.titleRatingsPath).getLines.toList.foldRight(
  //   Nil:List[TitleRatings])((line, acc) => ImdbData.parseTitleRatings(line) :: acc)

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewList: List[TitleCrew] = Source.fromFile(ImdbData.titleCrewPath).getLines.toList.map(ImdbData.parseTitleCrew)
  // val titleCrewList: List[TitleCrew] = Source.fromFile(ImdbData.titleCrewPath).getLines.toList.foldRight(
  //   Nil:List[TitleCrew])((line, acc) => ImdbData.parseTitleCrew(line) :: acc)

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsList: List[NameBasics] = Source.fromFile(ImdbData.nameBasicsPath).getLines.toList.map(ImdbData.parseNameBasics)
  // val nameBasicsList: List[NameBasics] = Source.fromFile(ImdbData.nameBasicsPath).getLines.toList.foldRight(
  //   Nil:List[NameBasics])((line, acc) => ImdbData.parseNameBasics(line) :: acc)
  
  //add more TitleBasics instances when more than one genres for a title
  def seperate_line_instance(instance: TitleBasics): List[TitleBasics] = instance.genres match {
    case Some(l) => l.map(g => TitleBasics(
      instance.tconst, instance.titleType, instance.primaryTitle, instance.originalTitle, instance.isAdult, 
      instance.startYear, instance.endYear, instance.runtimeMinutes, Some(List(g))))
    case None => List(instance)
  }

  //convert type of runtimeMinutes, startYear
  def change_type_int(x: Option[Int]): Int = x match {
    case Some(i) => i 
    case None => -1
  }

  //convert type of genres to String
  def change_type_string(x: Option[List[String]]): String = x match {
    case Some(i) => i.head
    case None => "None"
  }

  //convert type of primaryTitle to String
  def change_type_string2(x: Option[String]): String = x match {
      case Some(s) => s
      case None => "None" //which is not possible to reach as None type has already filtered 
  }
  //convert type of tconst list to a list of String
  def change_type_string3(x: Option[List[String]]): List[String] = x match {
    case Some(l) => l
    case None => List("None")
  }

  //continuously append Map(k: tconst, v: primaryTitle) into the whole Map
  def append_ms_l1(m: TitleBasics, ms: Map[String, String]): Map[String, String] = {
      val t = m.tconst
      val p_title = change_type_string2(m.primaryTitle)
      ms + (t -> p_title)
  }

  //continuously append Map(k: tconst, v: averageRating) into the whole Map
  def append_ms_l2(m: TitleRatings, ms: Map[String, Float]): Map[String, Float] = {
      val t = m.tconst
      val avg_r = m.averageRating
      ms + (t -> avg_r)
  }

  //continuously append Map(k: tconst, v: startYear) into the whole Map
  def append_ms_l3(m: TitleBasics, ms: Map[String, Option[Int]]): Map[String, Option[Int]] = {
      val t = m.tconst
      val s_year = m.startYear
      ms + (t -> s_year)
  }

  //get the decade based on its startYear
  def get_decade(year: Int): Int = {
    (year - 1900) / 10
  }

  //continuously compare the ratings and return tconst with higher rating
  def find_max_rating_helper(a: (String, Float), b: (String, Float)): (String, Float) = {
    if (a._2 > b._2) {a} 
    else if (a._2 == b._2) {
      //compare primaryTitle string, select the one comes first alphabetically
      if (a._1 < b._1) a else b
    } else {b}
  }
  
  //find the highest rating with the same decade and the same genre
  def find_max_rating(grouped: Map[String, List[String]], l1: Map[String, String],
                        l2: Map[String, Float]): Map[String, (String, Float)] = {
    val grouped_rating = grouped map {case (k, v) => (k -> v.map(t => (l1(t), l2(t))))}
    grouped_rating map {case (k, v) => (k -> v.reduce((x, y) => find_max_rating_helper(x, y)))}
  }

  //test whether the startYear of this tconst belongs to the range 
  def qualified(tconst: String, l1_map: Map[String, Option[Int]]): Boolean = {
    if (l1_map.keySet.contains(tconst) == false) {
      false
    } else {
      if (l1_map(tconst) == None) {
          false
      } else {
          if (change_type_int(l1_map(tconst)) >= 2010 && 
                change_type_int(l1_map(tconst)) <= 2021) true else false
      }
    }
  }

  def task1(list: List[TitleBasics]): List[(Float, Int, Int, String)] = {
    //remove rows with None values for runtimeMinutes column
    val re_list = list.filter(e => e.runtimeMinutes != None && e.genres != None)
    val extend_list = re_list.flatMap(seperate_line_instance)
    //only take intrested columns in tuples
    val tuple_list = extend_list.map(x => (change_type_string(x.genres), change_type_int(x.runtimeMinutes)))
    //group by their genres, also remove genres and only keep runtimeMinutes
    val grouped: Map[String, List[Int]] = tuple_list.groupBy(_._1).mapValues(t => {t.map(t => {t._2})})
    val avg_time = grouped map {case (k, v) => k -> (v.sum / v.size.toFloat)}
    val max_time = grouped map {case (k, v) => k -> (v.max)}
    val min_time = grouped map {case (k, v) => k -> (v.min)}
    //convert keys type to String
    val keys = grouped.keySet.toList
    keys.map(k => (avg_time(k), min_time(k), max_time(k), k))
  }

  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    //l1: remove rows with None values for startYear column,
    //and select rows with "movie" for titleType
    val re_l1 = l1.filter(e => e.startYear != None && e.titleType == Some("movie") && e.primaryTitle != None)
    val tuple_l1 = re_l1.map(x => (x.tconst, change_type_int(x.startYear)))
    val filtered_l1 = tuple_l1.filter(t => t._2 >= 1990 && t._2 <= 2018)
    //take filtered l1 tconst as list
    val tconst_l1 = filtered_l1.map(t => t._1)
    val filtered_l2 = l2.filter(x => x.averageRating >= 7.5 && x.numVotes >= 500000)
    //take filtered l2 tconst as list
    val tconst_l2 = filtered_l2.map(x => x.tconst)
    //intersect two tconst lists
    val tconst_l = tconst_l1 intersect tconst_l2
    //a Map contains all (k: tconst -> v: primaryTitle) from l1
    val map_l1 = re_l1.foldRight(Map[String, String]())((m, ms) => append_ms_l1(m, ms))
    //find corresponding title string gtom filetred tconst
    tconst_l.map(k => map_l1(k))
  }

  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = {
    //l1: remove rows with None values for startYear column
    val re_l1 = l1.filter(e => e.primaryTitle != None && e.genres != None &&
                             change_type_int(e.startYear) >= 1900 && change_type_int(e.startYear) <= 1999 && 
                             e.titleType == Some("movie"))
    
    //choose joint tconst - catch exception
    val tconst_l1 = re_l1.map(e => e.tconst)
    val tconst_l2 = l2.map(e => e.tconst)
    val tconst_joint = tconst_l1 intersect tconst_l2
    val joint_l1 = re_l1.filter(e => (tconst_joint.contains(e.tconst)))

    //deal with more than one genre for an object
    val extend_l1 = joint_l1.flatMap(seperate_line_instance)
    //only contain intrested columns of l1 into tuples
    val l1_tuples = extend_l1.map(x => (x.tconst, get_decade(change_type_int(x.startYear)), x.genres))

    //a Map contains all (k: tconst -> v: primaryTitle) from l1
    val map_l1 = extend_l1.foldRight(Map[String, String]())((m, ms) => append_ms_l1(m, ms))
    //a Map contains all (k: tconst -> v: averageRating) from l2
    val map_l2 = l2.foldRight(Map[String, Float]())((m, ms) => append_ms_l2(m, ms))
    
    //get a Map grouped by decade, and remove decades in values of Map
    val grouped_d: Map[Int, List[(String, String)]] = l1_tuples.groupBy(_._2).mapValues(
                                                    t => {t.map(t => (t._1, change_type_string(t._3)))})
    //get a Map(Map()) grouped by decade then genre, and remove genres in values 
    val grouped_d_g: Map[Int, Map[String, List[String]]] = grouped_d map {case (k, v) => 
                                                                            (k -> v.groupBy(_._2).mapValues(
                                                                             t => t.map(t => t._1)))}
    //Map(Map()) with the tconst who obtains the highest avgerage rating 
    val res_map: Map[Int, Map[String, (String, Float)]] = grouped_d_g map {case (k, v) => 
                                                        (k -> find_max_rating(v, map_l1, map_l2))}
    //sort by decade and then by genre
    val key_decade = res_map.keySet.toList.sorted
    val keys_pair = key_decade.flatMap(k1 => res_map(k1).keySet.toList.sorted.map(k2 => (k1, k2)))
    keys_pair.map(x => (x._1, x._2, res_map(x._1)(x._2)._1))
  }

  // Hint: There could be an input list that you do not really need in your implementation.
  def task4(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = {
    //remove None for intrested Option type in l3
    val l3_f = l3.filter(e => e.primaryName != None && e.knownForTitles != None)
    //put intrested columns into Map or tuple - for searching efficiency
    val l1_map = l1.foldRight(Map[String, Option[Int]]())((m, ms) => append_ms_l3(m, ms))
    val l3_tuple = l3_f.map(x => (change_type_string2(x.primaryName), change_type_string3(x.knownForTitles)))
    //find film counts for all tconst lists
    val film_cnt = l3_tuple.map(t => (t._1, t._2.filter(x => qualified(x, l1_map)).size))
    film_cnt.filter(_._2 >= 2)
  }

  def main(args: Array[String]) = {
    val durations = timed("Task 1", task1(titleBasicsList))
    val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
    val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    val crews = timed("Task 4", task4(titleBasicsList, titleCrewList, nameBasicsList))
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
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
