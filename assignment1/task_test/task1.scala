import javax.crypto.CipherInputStream
case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])

object TestCW{
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

  //add more TitleBasics instances when more than one genres for a title
  def seperate_line(line: TitleBasics): List[TitleBasics] = line.genres match {
    case Some(x) => x.map(g => TitleBasics(
      line.tconst, line.titleType, line.primaryTitle, line.originalTitle, line.isAdult, 
      line.startYear, line.endYear, line.runtimeMinutes, Some(List(g))))
    case None => List(line)
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

  def task1(list: List[TitleBasics]): List[(Float, Int, Int, String)] = {
    //remove rows with None values for runtimeMinutes column
    val re_list = list.filter(_.runtimeMinutes != None)
    val extend_list = re_list.flatMap(seperate_line_instance)
    //only take intrested columns in tuples
    val tuple_list = extend_list.map(x => (x.genres, change_type_int(x.runtimeMinutes)))
    //group by their genres, also remove genres and only keep runtimeMinutes
    val grouped: Map[Option[List[String]], List[Int]] = tuple_list.groupBy(_._1).mapValues(t => {t.map(t => {t._2})})
    val avg_time = grouped map {case (k, v) => change_type_string(k) -> (v.sum / v.size.toFloat)}
    val max_time = grouped map {case (k, v) => change_type_string(k) -> (v.max)}
    val min_time = grouped map {case (k, v) => change_type_string(k) -> (v.min)}
    //convert keys type to String
    val keys = grouped.keySet.toList.map(change_type_string(_))
    keys.map(k => (avg_time(k), max_time(k), min_time(k), k))
  }

  def main(args: Array[String]) = {
    val line = List(List("t01","movie","Name1","Name 1","0","1999","\\N","1","Documentary,Short"),
                    List("t02","movie","Name2","Name 2","0","1991","\\N","5","Animation,Short"),
                    List("t01","movie","Name1","Name 1","0","1999","\\N","\\N","justone"),
                    List("t01","movie","Name1","Name 1","0","1999","\\N","2","\\N")).map(stringsToTsv)
    val line_list = line.map(parseTitleBasics)
    //task1
    val results = task1(line_list)
    println(results)
  }
}