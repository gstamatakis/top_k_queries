package skyline

import cases.{PartitionedPoint, Point}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import partitioner.SkylinePartitioner

class SkylineCalc(spark: SparkSession, partitioner: SkylinePartitioner) extends Serializable {

    import spark.implicits._

    //BNL algorithm for skylines
    def BlockNestedLoop(dataset: Dataset[Point]): Vector[Point] = {

        /**
         * Calculates the local skyline of the input Iterator of Partitioned Points.
         *
         * @param ppi Input partitioned point iterator.
         * @return An Iterator with a *single* Vector of Points.
         */
        def myCombiner(ppi: Iterator[PartitionedPoint]): Iterator[Vector[Point]] = {
            //All points per partition
            val localSkyline: Vector[Point] = ppi.map(_.point).toVector

            //Points to remove from skyline (the dominated points)
            val dominatedPoints: Set[Point] = localSkyline.foldLeft(Set[Point]()) {
                (dominated: Set[Point], candidate: Point) => {
                    //Calculate all the points that need to be removed for this candidate
                    val pointsToRemove: Set[Point] =
                    //If we have already decided this point is dominated don't do anything
                        if (dominated.contains(candidate)) {
                            Set[Point]()
                        } else {
                            //Find all the points dominated by the candidate
                            val dominatedByCandidate: Set[Point] = localSkyline
                              .filter(p => candidate.dominates(p)) //Keep only the dominated points
                              .toSet[Point] //We need a set to avoid duplicates

                            //Find if the candidate is dominated by the skyline
                            if (localSkyline.exists(_.dominates(candidate))) {
                                dominatedByCandidate + candidate
                            } else {
                                dominatedByCandidate
                            }
                        }
                    //Extend the Set of dominated points with another Set
                    dominated.union(pointsToRemove)
                }
            }

            //Remove all the dominated points from the skyline
            val result: Vector[Point] = localSkyline.diff(dominatedPoints.toVector)
            Iterator[Vector[Point]](result)
        }

        //Used to calculate the global skyline
        def myReducer(v1: Vector[Point], v2: Vector[Point]): Vector[Point] = {
            //Null and empty checks
            if (v1 == null && v2 == null) {
                return Vector[Point]()
            }
            if (v1 == null) {
                return v2
            }
            if (v2 == null) {
                return v1
            }
            if (v1.isEmpty) {
                return v2
            }
            if (v2.isEmpty) {
                return v1
            }

            //Calculate the skyline of v2
            val final_v2: Vector[Point] = v1.foldLeft(v2) {
                (acc: Vector[Point], v1_point: Point) => {
                    //Keep all Points that aren't dominated by v1
                    acc.filter(v2_point => !v1_point.dominates(v2_point))
                }
            }

            //Calculate the skyline of v1
            val final_v1: Vector[Point] = v2.foldLeft(v1) {
                (acc: Vector[Point], v2_point: Point) => {
                    acc.filter(v1_point => !v2_point.dominates(v1_point))
                }
            }

            //Merge the 2 skylines
            final_v1 ++ final_v2
        }

        //Partitioned points
        val partitionedDataset: Dataset[PartitionedPoint] = dataset
          .map(p => PartitionedPoint(p, partitioner.partition(p)))
          .as[PartitionedPoint]

        //Local skylines per partition
        val localSkylines: Dataset[Vector[Point]] = partitionedDataset
          .repartitionByRange(col("partition"))
          .mapPartitions((ppi: Iterator[PartitionedPoint]) => myCombiner(ppi))

        //Combine the local skylines into a global skyline
        val globalSkyline: Vector[Point] = localSkylines
          .reduce((a: Vector[Point], b: Vector[Point]) => myReducer(a, b))

        //Return the skyline of the entire dataset
        globalSkyline
    }
}
