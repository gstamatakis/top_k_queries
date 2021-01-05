package cases

case class Point(dimensions: Vector[Double]) extends Serializable {

    def dim(i: Int): Double = dimensions.toList(i)

    //Number of dimensions
    def size: Int = dimensions.length

    //Check if this point dominates other point
    def dominates(other: Point): Boolean = {
        if (this.equals(other)) {
            false
        } else {
            this.dimensions
              .zip(other.dimensions)
              .foldLeft(true)(
                  (dominates: Boolean, d: (Double, Double)) => {
                      //Calculate (Ai,Bi) result
                      //compare returns (_<0,_=0,_>0)
                      //signum returns the sign of the input
                      //      (1,2) (2,3) (3,1) (1,5)
                      // true true  true  false false
                      d._1.compare(d._2).signum match {
                          case -1 => dominates
                          case 0 => dominates
                          case 1 => false
                      }
                  }
              )
        }
    }

    //Equals
    def equals(other: Point): Boolean = this.dimensions == other.dimensions
}
