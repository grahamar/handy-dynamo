package com.teambytes.handy.dynamo.replication

trait SqlDbApi[A] {
  def countDbObjects(): Long

  def loadDbObjects(page: Int, pageSize: Int): Iterable[A]
}
