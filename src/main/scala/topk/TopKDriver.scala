package topk

import java.time.{Duration, Instant}

import cases.{Point, TopKPoint}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import partitioner.{AnglePartitioner, RandomPartitioner}
import skyline.{STDAlgorithm, SkylineCalc, SkylineUtils}

object TopKDriver {
    def main(args: Array[String]): Unit = {
        //Keep track of elapsed time
        val start = Instant.now()

        val k = 5
        val numPartitions = 12
        val partitioner = "angle"

        val inFolder = "datasets/"
        val outFolder = "output/"
        val datasetName = "500000_3_N(0.5,0.1).csv"

        val datasetPath = inFolder + datasetName
        val outputPath = outFolder + System.nanoTime() + "_" + datasetName

        //Spark session object, use to communicate with Spark
        val ss = SparkSession
          .builder
          .config("spark.driver.host", "127.0.0.1")
          .master("local[*]")
          .appName(TopKDriver.getClass.getName)
          .getOrCreate()

        //implicits
        import ss.implicits._

        //Input dataset
        val dataset: Dataset[Point] = ss
          .read
          .option("header", "false")
          .option("mode", "DROPMALFORMED")
          .option("delimiter", ",")
          .option("inferSchema", "true")
          .textFile(datasetPath)
          .map(row => Point(row.split(",").map(el => el.toDouble).toVector))
          .as[Point]

        //Skyline utility functions
        val skylineUtils: SkylineUtils = new SkylineUtils(ss)

        //Select a partitioner
        val selPartitioner = if (partitioner.eq("angle")) {
            new AnglePartitioner(ss, dataset, numPartitions)
        } else {
            new RandomPartitioner(numPartitions)
        }

        //Skyline calculator
        val skylineCalculator: SkylineCalc = new SkylineCalc(ss, selPartitioner)

        //Select a Top-K Algorithm to use
        val topKAlgorithm = new STDAlgorithm(ss, k)

        //Top-K elements
        val topK: Vector[TopKPoint] = topKAlgorithm(dataset, skylineCalculator, skylineUtils)

        //Write these top K elements to the output dataset
        val topKDataset: Dataset[TopKPoint] = ss.createDataset(topK).sort("k")

        val chunks = 1
        topKDataset
          .coalesce(chunks)
          .map(p => p.k + "," + p.point.dimensions.map(d => f"$d%1.5f").mkString(","))
          .write
          .option("sep", ",")
          .option("quote", "")
          .mode(SaveMode.ErrorIfExists)
          .csv(outputPath)

        //Stop the spark session
        ss.stop()

        //Print the elapsed time
        println("Duration (ms): " + Duration.between(start, Instant.now()).toMillis)
    }
}
