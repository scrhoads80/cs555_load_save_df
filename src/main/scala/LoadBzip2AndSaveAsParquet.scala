import java.io.File

import org.apache.spark.sql.SparkSession

import scala.collection.immutable._

object LoadBzip2AndSaveAsParquet {

  def main(args: Array[String]): Unit = {

    if(args.length != 4) {
      println("invalid number of args")
      println("bzPath parquetOutputPath commaSeperated2digitMonts (e.g. 09,10) <local | yarn>")
      return
    }

    val bzPath = args(0)
    val parquetPath = args(1)
    val commaSeperatedMonths = args(2)
    val localOrYarn = args(3)

    val monthSeq = commaSeperatedMonths.split(',').toSeq

    val spark = if(localOrYarn == "local") {
      SparkSession.builder().appName("LoadBzIntoParquet").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("LoadBzIntoParquet").getOrCreate()
    }


    // create list of values for bz2 files
//    val sixToTwelveRange = Seq("06", "07", "08", "09", "10", "11", "12")

    val submissionStrList = monthSeq.map(x => "RS_2016-" + x + ".bz2")
    val commentStrList = monthSeq.map(x => "RC_2016-" + x + ".bz2")

    // read json file for submission
    submissionStrList.map(submissionFileBz2 => {
      readBzWriteParquet(submissionFileBz2, bzPath, parquetPath, spark)
    })

    commentStrList.map(readBzWriteParquet(_, bzPath, parquetPath, spark))

    spark.stop()
  }

  def readBzWriteParquet(submissionFileBz2: String, bzPath: String, parquetPath: String, spark: SparkSession): Unit = {
    val subDf = spark.read.json(bzPath + File.separator + submissionFileBz2)

    // save out to parquet
    subDf.write.parquet(parquetPath + File.separator + submissionFileBz2.split(".bz2")(0))
  }

}
