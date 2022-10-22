import javax.crypto.CipherInputStream
case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)

object Task2{
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

  //add more TitleBasics instances when more than one genres for a title
  def seperate_line_instance(instance: TitleBasics): List[TitleBasics] = instance.genres match {
    case Some(l) => l.map(g => TitleBasics(
      instance.tconst, instance.titleType, instance.primaryTitle, instance.originalTitle, instance.isAdult, 
      instance.startYear, instance.endYear, instance.runtimeMinutes, Some(List(g))))
    case None => List(instance)
  }

  // convert type of runtimeMinutes
  def change_type_int(x: Option[Int]): Int = x match {
    case Some(i) => i 
    case None => -1
  }
  // convert type of genres to String
  def change_type_string(x: Option[List[String]]): String = x match {
    case Some(i) => i.head
    case None => "None"
  }

  def change_type_string2(x: Option[String]): String = x match {
      case Some(s) => s
      case None => "None"
  }

  def append_mas(m: TitleBasics, ms: scala.collection.mutable.Map[String, String]):
       scala.collection.mutable.Map[String, String] = {
      val t = m.tconst
      val p_title = change_type_string2(m.primaryTitle)
      ms += (t -> p_title)
  }

  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    //l1: remove rows with None values for startYear column,
    //and select rows with "movie" for titleType
    val re_l1 = l1.filter(e => e.startYear != None && e.titleType == Some("movie"))
    val tuple_l1 = re_l1.map(x => (x.tconst, change_type_int(x.startYear)))
    val filtered_l1 = tuple_l1.filter(t => t._2 >= 1990 && t._2 <= 2018)
    //take filtered l1 tconst as list
    val tconst_l1 = filtered_l1.map(t => t._1)
    val tuple_l2 = l2.map(x => (x.tconst, x.averageRating, x.numVotes))
    val filtered_l2 = tuple_l2.filter(t => t._2 >= 7.5 && t._3 >= 500000)
    //take filtered l2 tconst as list
    val tconst_l2 = filtered_l2.map(t => t._1)
    //intersect two tconst lists
    val tconst_l = tconst_l1 intersect tconst_l2
    //a Map contains all (k: tconst -> v: primaryTitle) from l1
    val map_l1 = re_l1.foldRight(scala.collection.mutable.Map[String,String]())((m, ms) => append_mas(m, ms))
    //find corresponding title string gtom filetred tconst
    tconst_l.map(k => map_l1(k))
  }

  def main(args: Array[String]) = {
    val list1 = 
    List(List("t01","movie","Name1","Name 1","0","1999","\\N","1","Documentary,Short"),
        List("t02","movie","Name2","Name 2","0","1991","\\N","5","Animation,Short")
      ).map(stringsToTsv)
    val list2 = 
    List(List("t01","7.8","1156047"), 
      List("t02","8.2","300001")
      ).map(stringsToTsv)
    val l1 = list1.map(parseTitleBasics)
    val l2 = list2.map(parseTitleRatings)
    //task2
    val res = task2(l1, l2)
    print(res)
  }
}
