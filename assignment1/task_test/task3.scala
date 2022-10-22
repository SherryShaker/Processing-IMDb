import javax.crypto.CipherInputStream
case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)

object Task3{
  val SKIP_VAL = "\\N"

  def stringsToTsv(l: List[String]) = l.mkString("\t")

  def parseAttribute(word: String): Option[String] = 
    if(word == SKIP_VAL) None else Some(word)

  def parseTitleBasics(line: String): TitleBasics = {
    val attrs = line.split("\t").map(parseAttribute)
    if(attrs.length != 9)
      sys.error("Error in the format of `title.basics.tsv`.")
    TitleBasics(attrs(0).get, attrs(1), attrs(2), 
              attrs(3), attrs(4).get.toInt, attrs(5).map(_.toInt), attrs(6).map(_.toInt),
              attrs(7).map(_.toInt), attrs(8).map(_.split(",").toList))
  }
  def parseTitleRatings(line: String): TitleRatings = {
    val attrs = line.split("\t").map(parseAttribute)
    if(attrs.length != 3)
      sys.error("Error in the format of `title.ratings.tsv`.")
    TitleRatings(attrs(0).get, attrs(1).get.toFloat, attrs(2).get.toInt)
  }

  // convert type of runtimeMinutes, startYear
  def change_type_int(x: Option[Int]): Int = x match {
    case Some(i) => i 
    case None => -1
  }
  // convert type of genres to String
  def change_type_string(x: Option[List[String]]): String = x match {
    case Some(i) => i.head
    case None => "None"
  }
  //convert type of primaryTitle to String
  def change_type_string2(x: Option[String]): String = x match {
      case Some(s) => s
      case None => "None"
  }
  //add more TitleBasics instances when more than one genres for a title
  def seperate_line_instance(instance: TitleBasics): List[TitleBasics] = instance.genres match {
    case Some(l) => l.map(g => TitleBasics(
      instance.tconst, instance.titleType, instance.primaryTitle, instance.originalTitle, instance.isAdult, 
      instance.startYear, instance.endYear, instance.runtimeMinutes, Some(List(g))))
    case None => List(instance)
  }
  //continuously append Map(k: tconst, v: primaryTitle) into the whole Map
  def append_ms_l1(m: TitleBasics, ms: scala.collection.mutable.Map[String, String]):
       scala.collection.mutable.Map[String, String] = {
      val t = m.tconst
      val p_title = change_type_string2(m.primaryTitle)
      ms += (t -> p_title)
  }

  //continuously append Map(k: tconst, v: averageRating) into the whole Map
  def append_ms_l2(m: TitleRatings, ms: scala.collection.mutable.Map[String, Float]):
       scala.collection.mutable.Map[String, Float] = {
      val t = m.tconst
      val avg_r = m.averageRating
      ms += (t -> avg_r)
  }

  //get the decade based on its startYear
  def get_decade(year: Int): Int = {
    (year - 1900) / 10
  }

  // //continuously compare the ratings and return tconst with higher rating
  // def find_max_rating_helper(cur_tconst: String, cur_max_tconst: String, 
  //                               l1: scala.collection.mutable.Map[String, String], 
  //                               l2: scala.collection.mutable.Map[String, Float]): String = {
  //   val cur_r = l2(cur_tconst)
  //   val cur_max_r = l2(cur_max_tconst)
  //   if (cur_r > cur_max_r) {
  //     cur_tconst
  //   } else if (cur_r == cur_max_r) {
  //     //compare primaryTitle string, select the one comes first alphabetically
  //     if (l1(cur_tconst) < l1(cur_max_tconst)) cur_tconst else cur_max_tconst
  //   } else {
  //     cur_max_tconst
  //   }
  // }

//   def find_max_rating_helper1(tconst_l: List[String], l1: scala.collection.mutable.Map[String, String],
//                                 l2: scala.collection.mutable.Map[String, Float]): String = {
//       val rating_l = tconst_l.map(t -> l2(t))
//       rating_l.foldRight((0,0))(((r,idx), (max_r,max_idx)) => find_max_rating_helper2((r,idx), (max_r,idx), l1, l2))
//   }

//   //seperate out the inner Map, and find its tconst with highest rating as values
//   //NOTE: return results should contain only one tconst in each value list
//   def find_max_rating(grouped: Map[String, List[String]], l1: scala.collection.mutable.Map[String, String],
//                         l2: scala.collection.mutable.Map[String, Float]): Map[String, String] = {
//     val res = grouped map {case (k, v) => v.foldRight(0)((t, max_t) => 
//                                 find_max_rating_helper(t, max_t, l1, l2))}
//     // res map {case (k, v) => v.toString}\
//     // grouped map {case (k, v) => (k -> find_max_rating_helper1(v, l1, l2))}
//   }

  def find_max_rating_helper(a: (String, Float), b: (String, Float)): (String, Float) = {
    if (a._2 > b._2) {a} 
    else if (a._2 == b._2) {
      //compare primaryTitle string, select the one comes first alphabetically
      if (a._1 < b._1) a else b
    } else {b}
  }
  
  def find_max_rating(grouped: Map[String, List[String]], l1: scala.collection.mutable.Map[String, String],
                        l2: scala.collection.mutable.Map[String, Float]): Map[String, (String, Float)] = {
    val grouped_rating = grouped map {case (k, v) => (k -> v.map(t => (l1(t), l2(t))))}
    grouped_rating map {case (k, v) => (k -> v.reduce((x, y) => find_max_rating_helper(x, y)))}
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
    val map_l1 = extend_l1.foldRight(scala.collection.mutable.Map[String,String]())((m, ms) => append_ms_l1(m, ms))
    //a Map contains all (k: tconst -> v: averageRating) from l2
    val map_l2 = l2.foldRight(scala.collection.mutable.Map[String,Float]())((m, ms) => append_ms_l2(m, ms))
    
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

  // def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = {
  //   //l1: remove rows with None values for startYear column
  //   val re_l1 = l1.filter(e => e.primaryTitle != None && e.genres != None &&
  //                            change_type_int(e.startYear) >= 1900 && change_type_int(e.startYear) <= 1999 && 
  //                            e.titleType == Some("movie"))
  //   val extend_l1 = re_l1.flatMap(seperate_line_instance)
  //   //only contain intrested columns of l1 into tuples
  //   val l1_tuples = extend_l1.map(x => (x.tconst, get_decade(change_type_int(x.startYear)), x.genres))
  //   //a Map contains all (k: tconst -> v: primaryTitle) from l1
  //   val map_l1 = extend_l1.foldRight(scala.collection.mutable.Map[String,String]())((m, ms) => append_ms_l1(m, ms))
  //   //a Map contains all (k: tconst -> v: averageRating) from l2
  //   val map_l2 = l2.foldRight(scala.collection.mutable.Map[String,Float]())((m, ms) => append_ms_l2(m, ms))
  //   //get a Map grouped by decade, and remove decades in values of Map
  //   val grouped_d: Map[Int, List[(String, String)]] = l1_tuples.groupBy(_._2).mapValues(
  //                                                   t => {t.map(t => (t._1, change_type_string(t._3)))})
  //   //get a Map(Map()) grouped by decade then genre, and remove genres in values 
  //   val grouped_d_g: Map[Int, Map[String, List[String]]] = grouped_d map {case (k, v) => 
  //                                                                           (k -> v.groupBy(_._2).mapValues(
  //                                                                            t => t.map(t => t._1)))}
  //   //Map(Map()) with the tconst who obtains the highest avgerage rating 
  //   val res_map: Map[Int, Map[String, (String, Float)]] = grouped_d_g map {case (k, v) => 
  //                                                       (k -> find_max_rating(v, map_l1, map_l2))}
  //   //sort by decade and then by genre
  //   val key_decade = res_map.keySet.toList.sorted
  //   val keys_pair = key_decade.flatMap(k1 => res_map(k1).keySet.toList.sorted.map(k2 => (k1, k2)))
  //   keys_pair.map(x => (x._1, x._2, res_map(x._1)(x._2)._1))
  // }

  def main(args: Array[String]) = {
    val list1 = 
    List(List("t01","movie","Name1","Name 1","0","1999","\\N","1","Documentary,Short"),
        List("t02","movie","Name2","Name 2","0","1991","\\N","5","Animation,Short"),
        List("tt0000574",	"movie", "The Story of the Kelly Gang", "The Story of the Kelly Gang", "0", "1906",
        	"\\N", "70","Action,Adventure,Biography"),
        List("tt0000522",	"movie", "The Moonshiners", "The Moonshiners", "0",	"1905",	"\\N","\\N", "Action,Drama,Short")
      ).map(stringsToTsv)
    val list2 = 
    List(List("t01","7.8","1156047"), 
      List("tt0000574",	"6.1", "695"),
      List("tt0000522", "5.0","1000")
      ).map(stringsToTsv)
    val l1 = list1.map(parseTitleBasics)
    val l2 = list2.map(parseTitleRatings)
    //task3
    val res = task3(l1, l2)
    println(res)
  }
}