package com.graphanalytics.util

object TimeUtils {
  def timed[A](fn: => A): (A, Long) = {
    val start = System.nanoTime()
    val result = fn
    val elapsedMs = (System.nanoTime() - start) / 1000000L
    (result, elapsedMs)
  }
}
