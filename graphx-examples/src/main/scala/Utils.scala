object Utils {
  def extremeN[T](n: Int, li: List[T])
                 (comp1: ((T, T) => Boolean), comp2: ((T, T) => Boolean)):
  List[T] = {

    def sortedIns(el: T, list: List[T]): List[T] =
      if (list.isEmpty) List(el) else if (comp2(el, list.head)) el :: list else
        list.head :: sortedIns(el, list.tail)

    def updateSofar(sofar: List[T], el: T): List[T] =
      if (comp1(el, sofar.head))
        sortedIns(el, sofar.tail)
      else sofar

    (li.take(n).sortWith(comp2(_, _)) /: li.drop(n)) (updateSofar(_, _))
  }

  def top[T](n: Int, li: List[T])
            (implicit ord: Ordering[T]): Iterable[T] = {
    extremeN(n, li)(ord.lt(_, _), ord.gt(_, _))
  }

  def bottom[T](n: Int, li: List[T])
               (implicit ord: Ordering[T]): Iterable[T] = {
    extremeN(n, li)(ord.gt(_, _), ord.lt(_, _))
  }
}