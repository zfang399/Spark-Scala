import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable.Map
import scala.tools.ant.sabbus.Break

object FP_Growth {
  def main(args: Array[String]) = {
    val sup_pct = 0.85
    val pnum = 8
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val tot_num = sc.textFile(args(0))

    val trans = sc.textFile(args(0)).map(line => (line.split(" ").drop(1).toList.sortWith(_ < _), 1)).reduceByKey(_ + _)

    //compute the support rate of every item
    val g = trans.flatMap(line => {
      var l = List[(String, Int)]()
      for (i <- line._1) {
        l = (i, line._2) :: l
      }
      l
    }).reduceByKey(_ + _).sortBy(_._2, false).cache()

    //give the items ids
    val g_list = g.collect.toArray
    var g_count = 0
    val g_map = Map[String, Int]()
    for (i <- g_list) {
      g_count += 1
      g_map(i._1) = g_count
    }

    //item's key: serial numbers of item
    var item = trans.map(line => {
      var l = List[Int]()
      for (i <- line._1) {
        l = g_map(i) :: l
      }
      (l.sortWith(_ < _), line._2)
    })

    //compute minimum support num
    val sup_num: Int = item.map(t => (1, t._2))
      .reduceByKey(_ + _)
      .first()._2 * sup_pct toInt

    val g_size = (g_count + pnum - 1) / pnum

    //group the items
    val f_list = item.flatMap(t => {
      var pre = -1
      var i = t._1.length - 1
      var result = List[(Int, (List[Int], Int))]()
      while (i >= 0) {
        if ((t._1(i) - 1) / g_size != pre) {
          pre = (t._1(i) - 1) / g_size
          result = (pre, (t._1.dropRight(t._1.length - i - 1), t._2)) :: result
        }
        i -= 1
      }
      result
    }).groupByKey().cache()

    val d_result = f_list.flatMap(t => {
      fp_growth(t._2, sup_num, t._1 * g_size + 1 to (((t._1 + 1) * g_size) min g_count))
    })

    val temp_result = d_result.map(t => (t._1.map(a => g_list(a - 1)._1), t._2))
    val result = temp_result.map(t => (listtostring(t._1)._2, listtostring(t._1)._1 + ":" + t._2.toFloat / tot_num.toFloat)).groupBy(_._1)
    result.map(t => t._2.map(s => s._2)).saveAsTextFile(args(1))

    sc.stop()
  }

  def listtostring(l: List[String]): (String, Int) = {
    var ret = ""
    var count = 0
    for (s <- l) {
      ret += s + ","
      count += 1
    }
    ret = "\r\n" + ret.substring(0, ret.size - 1)
    return (ret, count)
  }

  def fp_growth(v: Iterable[(List[Int], Int)], min_support: Int, target: Iterable[Int] = null): List[(List[Int], Int)] = {
    val root = new tree(null, null, 0)
    val tab = Map[Int, tree]()
    val tabc = Map[Int, Int]()
    //make tree
    for (i <- v) {
      var cur = root;
      var s: tree = null
      var list = i._1
      while (!list.isEmpty) {
        //if does not exist, add
        if (!tab.exists(_._1 == list(0))) {
          tab(list(0)) = null
        }
        if (!cur.son.exists(_._1 == list(0))) {
          //add the new point
          s = new tree(cur, tab(list(0)), list(0))
          tab(list(0)) = s
          cur.son(list(0)) = s
        } else {
          //proceed
          s = cur.son(list(0))
        }
        s.support += i._2
        cur = s
        list = list.drop(1)

      }
    }
    //prefix cut
    for (i <- tab.keys) {
      var count = 0
      var cur = tab(i)
      while (cur != null) {
        count += cur.support
        cur = cur.Gnext
      }
      //modify
      tabc(i) = count
      if (count < min_support) {
        var cur = tab(i)
        while (cur != null) {
          var s = cur.Gnext
          cur.Gparent.son.remove(cur.Gv)
          cur = s
        }
        tab.remove(i)
      }
    }
    //deal with target
    var r = List[(List[Int], Int)]()
    var tail: Iterable[Int] = null
    if (target == null)
      tail = tab.keys
    else {
      tail = target.filter(a => tab.exists(b => b._1 == a))
    }
    if (tail.count(t => true) == 0)
      return r
    //deal with the single branch
    var cur = root
    var c = 1
    while (c < 2) {
      c = cur.son.count(t => true)
      if (c == 0) {
        var res = List[(Int, Int)]()
        while (cur != root) {
          res = (cur.Gv, cur.support) :: res
          cur = cur.Gparent
        }

        val part = res.partition(t1 => tail.exists(t2 => t1._1 == t2))
        val p1 = gen(part._1)
        if (part._2.length == 0)
          return p1
        else
          return decare(p1, gen(part._2)) ::: p1
      }
      cur = cur.son.values.head
    }
    //mining the frequent itemset
    for (i <- tail) {
      var result = List[(List[Int], Int)]()
      var cur = tab(i)
      while (cur != null) {
        var item = List[Int]()
        var s = cur.Gparent
        while (s != root) {
          item = s.Gv :: item
          s = s.Gparent
        }
        result = (item, cur.support) :: result
        cur = cur.Gnext
      }
      r = (List(i), tabc(i)) :: fp_growth(result, min_support).map(t => (i :: t._1, t._2)) ::: r

    }
    r
  }

  def gen(tab: List[(Int, Int)]): List[(List[Int], Int)] = {
    if (tab.length == 1) {
      return List((List(tab(0)._1), tab(0)._2))
    }
    val sp = tab(0)
    val t = gen(tab.drop(1))
    return (List(sp._1), sp._2) :: t.map(s => (sp._1 :: s._1, s._2 min sp._2)) ::: t
    //TODO: sp._2 may not be min
  }

  //cartesian product
  def decare[T](a: List[(List[T], Int)], b: List[(List[T], Int)]): List[(List[T], Int)] = {
    var res = List[(List[T], Int)]()
    for (i <- a)
      for (j <- b)
        res = (i._1 ::: j._1, i._2 min j._2) :: res
    res
  }


  class tree(parent: tree, next: tree, v: Int) {
    val son = Map[Int, tree]()
    var support = 0

    def Gparent = parent

    def Gv = v

    def Gnext = next
  }
}
