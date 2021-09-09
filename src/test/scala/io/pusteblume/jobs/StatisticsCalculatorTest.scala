package io.pusteblume.jobs

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest._
import flatspec._
import io.pusteblume.jobs.StatisticsCalculator.{SensorStat, SensorStatValue, mStats, sensorStatisticsCalculator, sensorStatsOrder}
import matchers._
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.Await

class StatisticsCalculatorTest extends AnyFlatSpec with should.Matchers {
  implicit val system = ActorSystem("StatisticsCalculatorTest")
  implicit val executor = system.getDispatcher

  "mStats" should "increment total count for any measurement and failedCount only for failures" in {
    val future = Source(List(("id1", None), ("id1", Some(5)))).runWith(mStats)
    val result = Await.result(future, 3.seconds)
    assert(result == (2,1))
  }

  "sensorStatisticsCalculator" should "reduce a steam of sensor measurements to an aggregate per sensor" in {
    val future = Source(List(
      ("id1", None),
      ("id1", Some(5)),
      ("id3", None),
      ("id3", None),
      ("id2", Some(3)),
      ("id2", Some(4))
    )).via(sensorStatisticsCalculator).runWith(Sink.seq)
    val result = Await.result(future, 3.seconds)
    result should contain theSameElementsAs List(
      SensorStat("id1",Some(SensorStatValue(1,5,5,5.0))),
      SensorStat("id2",Some(SensorStatValue(2,3,4,3.5))),
      SensorStat("id3",None)
    )
  }

  "sensorStatsOrder" should "sort descending with tail failures" in {
    val unsorted = List(
      SensorStat("id3",None),
      SensorStat("id2",Some(SensorStatValue(2,3,4,3.5))),
      SensorStat("id1",Some(SensorStatValue(1,5,5,5.0)))
    )
    val sorted = List(
      SensorStat("id1",Some(SensorStatValue(1,5,5,5.0))),
      SensorStat("id2",Some(SensorStatValue(2,3,4,3.5))),
      SensorStat("id3",None),
    )

    unsorted.sortWith(sensorStatsOrder) should contain theSameElementsInOrderAs sorted
  }
}
