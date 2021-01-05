package skyline

import cases.{Point, PointWithDom, TopKPoint}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

class STDAlgorithm(ss: SparkSession, k: Int) extends Serializable {

    def apply(dataset: Dataset[Point], skylineCalculator: SkylineCalc, skylineUtils: SkylineUtils): Vector[TopKPoint] = {
        //Step 1 - Calculate the skyline of input dataset
        val skyline: Vector[Point] = skylineCalculator.BlockNestedLoop(dataset)

        //Step 2 - Find the score of each skyline point
        val skylineWithScore: Vector[PointWithDom] = skylineUtils.calcDomScore(skyline, dataset)

        //Step 3 - Keep the skyline points ordered by their score
        val PQ = new mutable.PriorityQueue[PointWithDom]()
        PQ ++= skylineWithScore

        //Input dataset, originally contains all points
        var currentDataset: Dataset[Point] = dataset

        //Main loop. Repeat k times
        1.to(math.min(k, PQ.size))
          .foldLeft(Vector[TopKPoint]())(
              (topKPoints: Vector[TopKPoint], k: Int) => {
                  //Retrieve the current top point from the PQ
                  val curTopK: PointWithDom = PQ.dequeue()

                  //Step 4 - Calculate the exclusive dominance region of the current top-k point
                  //Remove the topK point from the dataset
                  currentDataset = currentDataset.filter(p => !p.equals(curTopK.point))

                  //Prepare the skyline and current Top-K Point for broadcast
                  val curSkylineBV: Broadcast[Vector[PointWithDom]] = ss.sparkContext.broadcast(PQ.toVector)
                  val curTopBV: Broadcast[PointWithDom] = ss.sparkContext.broadcast(curTopK)

                  //Calculate the exclusive dom region
                  val exclusiveDomRegion: Dataset[Point] = currentDataset
                    .filter(dp => curTopBV.value.point.dominates(dp)) //Dominated by the current top K point
                    .filter(dp => !curSkylineBV.value.exists(sp => sp.point.dominates(dp))) //Not dominated by skyline

                  //Perform a local skyline query on the dom region
                  val domRegionSkyline: Vector[Point] = skylineCalculator.BlockNestedLoop(exclusiveDomRegion)

                  //Step 5 - Calculate the score of the region
                  val domRegionSkylineWithDom: Vector[PointWithDom] = skylineUtils.calcDomScore(domRegionSkyline, currentDataset)

                  //Add the exclusive region skyline to the PQ
                  PQ ++= domRegionSkylineWithDom

                  //Append the new Top-K point to the acc (Step 6)
                  topKPoints :+ TopKPoint(k, curTopK.point)
              }
          )

    }

}
