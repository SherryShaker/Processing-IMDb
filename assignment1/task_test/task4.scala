import javax.crypto.CipherInputStream
case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object Task4{
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
  def parseTitleCrew(line: String): TitleCrew = {
    val attrs = line.split("\t").map(parseAttribute)
    if(attrs.length != 3)
      sys.error("Error in the format of `title.crew.tsv`.")
    TitleCrew(attrs(0).get, attrs(1).map(_.split(",").toList), attrs(2).map(_.split(",").toList))
  }
  def parseNameBasics(line: String): NameBasics = {
    val attrs = line.split("\t").map(parseAttribute)
    if(attrs.length != 6)
      sys.error("Error in the format of `name.basics.tsv`.")
    NameBasics(attrs(0).get, attrs(1), attrs(2).map(_.toInt), attrs(3).map(_.toInt),
               attrs(4).map(_.split(",").toList), attrs(5).map(_.split(",").toList))
  }

  // convert type of runtimeMinutes, startYear
  def change_type_int(x: Option[Int]): Int = x match {
    case Some(i) => i 
    case None => -1
  }

  //convert type of primaryTitle to String
  def change_type_string2(x: Option[String]): String = x match {
      case Some(s) => s
      case None => "None"
  }

  // convert type of tconst list to a list of String
  def change_type_string3(x: Option[List[String]]): List[String] = x match {
    case Some(l) => l
    case None => List("None")
  }

  //continuously append Map(k: tconst, v: startYear) into the whole Map
  def append_ms_l3(m: TitleBasics, ms: Map[String, Option[Int]]):
      Map[String, Option[Int]] = {
      val t = m.tconst
      val s_year = m.startYear 
      ms + (t -> s_year)
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
    val titleBasicsData = 
    List(List("t01","movie","Name1","Name 1","0","1999","\\N","1","Documentary,Short"),
        List("t02","movie","Name2","Name 2","0","1991","\\N","5","Animation,Short"),
      ).map(stringsToTsv)
    val titleBasicsData2 = 
    List(List("t03","movie","Name3","Name 3","0","2010","\\N","1","Documentary,Short"),
        List("t04","movie","Name4","Name 4","0","2018","\\N","5","Animation,Short"),
        List("t05","movie","Name5","Name 5","0","2019","\\N","3","Animation,Short")
      ).map(stringsToTsv)
    val titleCrewData = 
    List(List("t01", "\\N", "nm01,nm02"),
        List("t02", "\\N", "nm04"),
        List("t03", "\\N", "nm02,nm03"),
        List("t04", "\\N", "nm01,nm03"),
        List("t05", "\\N", "nm02,nm03")
    ).map(stringsToTsv)
    val nameBasicsData = 
    List(List("nm01", "FName1 SName1", "\\N", "\\N", "miscellaneous", "t01,t04"),
        List("nm02", "FName2 SName2", "\\N", "\\N", "miscellaneous", "t01,t03,t05"),
        List("nm03", "FName3 SName3", "\\N", "\\N", "miscellaneous", "t03,t04,t05"),
        List("nm04", "FName4 SName4", "\\N", "\\N", "miscellaneous", "t02"),
        List("nm0054232", "Joe Barden",	"\\N", "\\N",	"art_department,special_effects", "tt2452386,tt3315342,tt0120669,tt1189340")
    ).map(stringsToTsv)
    val list1 = (titleBasicsData ++ titleBasicsData2).map(parseTitleBasics)
    val list2 = titleCrewData.map(parseTitleCrew)
    val list3 = nameBasicsData.map(parseNameBasics)
    //task4
    val res = task4(list1, list2, list3)
    println(res)
  }
}