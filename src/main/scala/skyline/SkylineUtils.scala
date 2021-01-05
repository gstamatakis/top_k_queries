package skyline

import cases.{Point, PointWithDom}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

class SkylineUtils(ss: SparkSession) extends Serializable {

    import ss.implicits._

    def calcDomScore(skyline: Vector[Point], dataset: Dataset[Point]): Vector[PointWithDom] = {

        def updateScore(pointWithDom: PointWithDom, inputPoint: Point): PointWithDom = {
            if (pointWithDom.point.dominates(inputPoint)) {
                PointWithDom(pointWithDom.point, pointWithDom.score + 1)
            } else {
                pointWithDom
            }
        }

        def mapperFunc(skylineBV: Broadcast[Vector[Point]])(partitionPoints: Iterator[Point]): Iterator[Vector[PointWithDom]] = {
            val localSkyline: Vector[Point] = skylineBV.value
            val localSkylineWithScores: Vector[PointWithDom] = localSkyline.map(p => PointWithDom(p, 0L))
            partitionPoints.map(p => localSkylineWithScores.map(pwd => updateScore(pwd, p)))
        }

        //Prepare the skyline points for broadcast
        val skylineBV: Broadcast[Vector[Point]] = ss.sparkContext.broadcast(skyline)

        //Skylines with dom score per partition
        val localSkylinesWithDom: Dataset[Vector[PointWithDom]] = dataset.mapPartitions(mapperFunc(skylineBV))

        //Skyline with dom score
        //eg: [(p1,4)(p2,0)],[(p1,2)(p2,4)] => [(p1,6)(p2,4)]
        localSkylinesWithDom.reduce((s1: Vector[PointWithDom], s2: Vector[PointWithDom]) => {
            s1.zip(s2).map(pair => PointWithDom(pair._1.point, pair._1.score + pair._2.score))
        })
    }
}
