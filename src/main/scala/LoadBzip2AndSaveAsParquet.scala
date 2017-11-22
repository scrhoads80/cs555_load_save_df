import java.io.File

import org.apache.spark.sql.SparkSession

import scala.collection.immutable._

object LoadBzip2AndSaveAsParquet {

  def main(args: Array[String]): Unit = {
    val path = args(0)

    val spark = SparkSession.builder().appName("Load And Save Bz2 Files").config("spark.master", "local[4]").getOrCreate()

    // create list of values for bz2 files
//    val sixToTwelveRange = Seq("06", "07", "08", "09", "10", "11", "12")
    val sixToTwelveRange = Seq("09", "10")

    val submissionStrList = sixToTwelveRange.map(x => "RS_2016-" + x + ".bz2")
    val commentStrList = sixToTwelveRange.map(x => "RC_2016-" + x + ".bz2")

    // read json file for submission
    submissionStrList.map(submissionFileBz2 => {
      readBzWriteParquet(submissionFileBz2, path, spark)
    })

    commentStrList.map(readBzWriteParquet(_, path, spark))

    spark.stop()
  }

  def readBzWriteParquet(submissionFileBz2: String, path: String, spark: SparkSession): Unit = {
    val subDf = spark.read.json(path + File.separator + submissionFileBz2)

    // save out to parquet
    subDf.write.parquet(path + File.separator + "parquet" + File.separator + submissionFileBz2.split(".bz2")(0))
  }

}
