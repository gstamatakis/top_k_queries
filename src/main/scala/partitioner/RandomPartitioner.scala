package partitioner

import cases.Point

class RandomPartitioner(partitions: Int) extends SkylinePartitioner {

    override def partition(point: Point): Int = {
        scala.util.Random.nextInt(partitions)
    }
}
