package cases

case class PointWithDom(point: Point, score: Long) extends Serializable with Comparable[PointWithDom] {

    override def compareTo(other: PointWithDom): Int = (this.score - other.score).signum

}
