package io.epiphanous.flinkrunner.flink.state


class ListState[T] extends Serializable {

  private var state : org.apache.flink.api.common.state.ListState[T] = _

  def apply(value: org.apache.flink.api.common.state.ListState[T]): Unit = {
    this.state = value
  }

  def isEmpty: Boolean = !this.state.get().iterator().hasNext

  def contains(element : T) : Boolean = {
    if(this.isEmpty) {
      return false
    }
    this.state.get().forEach(
      item => {
        if(item.equals(element)) {
          return true
        }
      }
    )
    false
  }

  def update(elements : java.util.List[T]) = this.state.update(elements)
  def add(element : T): Unit = this.state.add(element)
  def addAll(elements : java.util.List[T]) = this.state.addAll(elements)
}
