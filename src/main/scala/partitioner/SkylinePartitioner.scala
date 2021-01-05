package partitioner

import cases.Point

trait SkylinePartitioner extends Serializable {

    //Returns the partition ID of a given Point
    def partition(point: Point): Int
}
