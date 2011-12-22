import org.gridgain.scalar.scalar
import org.gridgain.scalar.scalar._
import org.gridgain.grid.GridClosureCallMode._
 
 
object Leaker {
    val str0 = "0"
    val str1 = "1"
    val str2 = "2"
    val str3 = "3"
    val str4 = "4"
    val str5 = "5"
    val str6 = "6"
    val str7 = "7"
    val str8 = "8"
    val str9 = "9"
 
    val int0 = 0
    val int1 = 1
    val int2 = 2
    val int3 = 3
    val int4 = 4
    val int5 = 5
    val int6 = 6
    val int7 = 7
    val int8 = 8
    val int9 = 9
 
    val flt0 = 0.0f
    val flt1 = 1.0f
    val flt2 = 2.0f
    val flt3 = 3.0f
    val flt4 = 4.0f
    val flt5 = 5.0f
    val flt6 = 6.0f
    val flt7 = 7.0f
    val flt8 = 8.0f
    val flt9 = 9.0f
 
    val dbl0 = 0.0
    val dbl1 = 1.0
    val dbl2 = 2.0
    val dbl3 = 3.0
    val dbl4 = 4.0
    val dbl5 = 5.0
    val dbl6 = 6.0
    val dbl7 = 7.0
    val dbl8 = 8.0
    val dbl9 = 9.0
 
 
    def test(i: Int) {
        println("Test " + i)
    }
}
 
object PermGenLeak {
    def main(args: Array[String]) {
        (0 until 3).foreach(i => {
            scalar {
                grid$.run$(BROADCAST, () => Leaker.test(i))
            }
        })
    }
}
