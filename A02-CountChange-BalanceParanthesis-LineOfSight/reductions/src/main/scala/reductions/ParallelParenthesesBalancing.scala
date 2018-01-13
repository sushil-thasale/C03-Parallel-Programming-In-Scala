package reductions

import scala.annotation._
import org.scalameter._
import common._


object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }

}


object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {

    @tailrec
    def balanceIter(chars: Array[Char], acc: Int): Boolean = {
      if (chars.isEmpty) acc == 0
      else if (chars.head == '(') balanceIter(chars.tail, acc + 1)
      else if (chars.head == ')' && acc > 0) balanceIter(chars.tail, acc - 1)
      else if (chars.head == ')' && acc == 0) false
      else balanceIter(chars.tail, acc)
    }

    balanceIter(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @tailrec
    def traverse(idx: Int, until: Int, acc: Int, sign: Int): (Int, Int) = {
      if (idx >= until) (acc, sign)
      else {
        val (newAcc, newSign) = chars(idx) match {
          case '(' => (acc + 1, if (sign == 0) sign + 1 else sign)
          case ')' => (acc - 1, if (sign == 0) sign - 1 else sign)
          case _ => (acc, sign)
        }
        traverse(idx + 1, until, newAcc, newSign)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val middle = from + (until - from) / 2
        val (resL, resR) = parallel(reduce(from, middle), reduce(middle, until))
        (Math.min(resL._1, resL._2 + resR._1), resL._2 + resR._2)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

}