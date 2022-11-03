package io.epiphanous.flinkrunner.flink.state


class ListState[T] {

  var state : org.apache.flink.api.common.state.ListState[T] = _

  def apply(value: org.apache.flink.api.common.state.ListState[T]): Unit = {
    this.state = value
  }

  def isEmpty: Boolean = this.state.get().iterator().hasNext

  def contains(element : T) : Boolean = {
    this.state.get().forEach(
      item => {
        if(item.equals(element)) {
          return true
        }
      }
    )
    false
  }
}
