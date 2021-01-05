package partitioner

import cases.Point
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{Dataset, SparkSession}

class AnglePartitioner(ss: SparkSession, dataset: Dataset[Point], numPartitions: Int, sampleFraction: Double = 0.05) extends SkylinePartitioner {

    import ss.implicits._

    private val sampledDataset = dataset
      .sample(sampleFraction)
      .map(p => math.atan(p.dim(1) / p.dim(0)))

    private val sampledSeq = sampledDataset
      .agg(min(sampledDataset.columns(0)), max(sampledDataset.columns(0)))
      .first()

    private val full_angle = Math.PI / 2
    private val min_angle: Double = sampledSeq.getAs[Double](0)
    private val max_angle: Double = sampledSeq.getAs[Double](1)

    private val dead_space = (full_angle - max_angle) + min_angle
    private val effective_angle = full_angle - dead_space

    override def partition(point: Point): Int = {
        val angle = math.atan(point.dim(1) / point.dim(0)) - dead_space
        val normalized_angle = angle / effective_angle
        (normalized_angle * numPartitions).toInt
    }
}
