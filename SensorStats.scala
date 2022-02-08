import java.io.File
import java.nio.file.Files
import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    val files = getListOfFiles(new File(args(0)))
    val fileData = files.map (fileName => Source.fromFile(fileName).getLines )
    val dataToProcess = fileData.flatMap(line => line.toList)
    val sensorWiseData = getSensorWiseData(dataToProcess)
    printOutput(files.size, sensorWiseData.flatMap(_._2).size, sensorWiseData.flatMap(_._2.filter(_ == "NaN")).size, getMinAvgMax(sensorWiseData))
  }

  def printOutput(filesProcessed: Int, processedData: Int, failedData: Int, minAvgMaxSensorData: Map[String, (Any, Any, Any)]) ={
    println("Num of processed files: "+ filesProcessed)
    println("Num of processed measurements: "+ processedData)
    println("Num of failed measurements: "+ failedData)
    println("Sensors with highest avg humidity:\n\nsensor-id,min,avg,max")
    val formatted = minAvgMaxSensorData.map(a=>(a._1, a._2._1.toString, a._2._2.toString, a._2._3.toString))
    formatted.toList.sorted.foreach(l => println(s"""${l._1},${l._2},${l._3},${l._4} """))
  }

  def getMinAvgMax(data: Map[String, List[String]]): Map[String, (Any, Any, Any)] = {
    data.map(kv =>
      kv._2 match {
        case l:List[String] if(l.size == 1 & l.contains("NaN")) => (kv._1,("NaN","NaN","NaN"))
        case m:List[String] if(m.size > 1) =>
          val kv2Filtered = kv._2.filterNot(_ == "NaN").map(_.toInt)
          (kv._1, (kv2Filtered.min, getAvg(kv2Filtered) ,kv2Filtered.max))
      })
  }

  def getAvg(a: List[Int]): Int = a.sum/a.size

  def getSensorWiseData(dataList: List[String]): Map[String, List[String]] = {
    dataList.filterNot(_ == "sensor-id,humidity").map { data =>
        val sensorDataArray = data.split(",")
        (sensorDataArray(0), sensorDataArray(1))
    }.groupBy(_._1).map(a=> (a._1, a._2.map(_._2)))
  }

  def getListOfFiles(dir: File):List[File] = dir.listFiles.filter(_.isFile).toList

}
