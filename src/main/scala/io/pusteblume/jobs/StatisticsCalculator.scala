package io.pusteblume.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, RunnableGraph, Sink, Source}
import akka.util.ByteString
import cats.effect._
import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.effect._

import java.nio.file.{Files, Path}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object StatisticsCalculator extends CommandIOApp(
  name = "sensor-statistics",
  header = "Calculates statistics for sensor data",
  version = "0.0.1"
) {
  implicit val system = ActorSystem("StatisticsCalculator")
  implicit val executor = system.getDispatcher

  override def main: Opts[IO[ExitCode]] = pathOpt.map(
    path => {
      val graph = getGraph(getFileSource(path), fStats, mStats, sStats)
      IO.fromFuture {
        IO {
          val graphRes = graph.run()
          val res = for {
            n1 <- graphRes._1
            n2 <- graphRes._2
            n3 <- graphRes._3
          } yield (n1, n2, n3)

          res
            .andThen { s =>
              s match {
                case Success((fileStat, mesurementStat, sensorStat)) =>
                  println(System.lineSeparator)
                  println(s"Num of processed files: ${fileStat}")
                  println(s"Num of processed measurements: ${mesurementStat._1}")
                  println(s"Num of failed measurements: ${mesurementStat._2}")
                  println(System.lineSeparator)
                  println("Sensors with highest avg humidity:")
                  println("sensor-id,min,avg,max")
                  sensorStat
                    .sortWith(sensorStatsOrder)
                    .map {
                      case SensorStat(id, Some(SensorStatValue(_, min, max, avg))) => s"$id,$min,$avg,$max"
                      case SensorStat(id, None) => s"$id,NaN,NaN,NaN"
                    }
                    .foreach(println)
                case Failure(exception) =>
                  println("Error calculating statistics.")
                  exception.printStackTrace()
              }
              system.terminate()
            }
            .transform {
              case Success(_) =>
                Try(ExitCode.Success)
              case Failure(_) =>
                Try(ExitCode.Error)
            }

        }
      }
    }
  )

  val sensorStatsOrder: (SensorStat, SensorStat) => Boolean = (a: SensorStat, b: SensorStat) =>
    (a.value, b.value) match {
      case (Some(SensorStatValue(_, _, _, a1)), Some(SensorStatValue(_, _, _, a2))) => a1 > a2
      case (Some(_), None) => true
      case _ => false
    }

  val pathOpt: Opts[Path] = Opts
    .option[Path]("data-files", "Path to directory containing data files.")
    .validate("Should be a valid path!")(Files.isDirectory(_))

  val fStats: Sink[Path, Future[Int]] = Sink
    .fold(0)((acc, _) => acc + 1)

  val mStats: Sink[Measurement, Future[(Long, Long)]] = Sink
    .fold((0L, 0L)) { (acc, m) =>
      val failedIncrement = m match {
        case (_, None) => 1L
        case _ => 0L
      }
      (acc._1 + 1L, acc._2 + failedIncrement)
    }

  val sStats: Sink[SensorStat, Future[Seq[SensorStat]]] = Sink.seq

  val sensorStatisticsCalculator = Flow[Measurement]
    .groupBy(Int.MaxValue, m => m._1)
    .fold(SensorStat("", None)) { (a, m) =>
      val value = (a.value, m._2) match {
        case (Some(SensorStatValue(aCount, aMin, aMax, aAvg)), Some(mes)) =>
          val newCount = aCount + 1
          val newVal = SensorStatValue(newCount, Math.min(aMin, mes), Math.max(aMax, mes), aAvg + (mes - aAvg) / newCount)
          Some(newVal)
        case (_@s, None) => s
        case (None, Some(mes)) => Some(SensorStatValue(1, mes, mes, mes))
      }

      SensorStat(m._1, value)
    }
    .async
    .mergeSubstreams

  val fileToContentStringConverter = Flow[Path]
    .flatMapConcat { path =>
      FileIO
        .fromPath(path)
        .via(Framing.delimiter(ByteString(System.lineSeparator), 10000, allowTruncation = true))
        .drop(1)
        .map(_.utf8String)
        .map(line => {
          val Array(id, v, _*) = line.split(",")
          (id, if (v === "NaN") None else Some(Integer.parseInt(v)))
        })
    }

  def getFileSource(path: Path): Source[Path, NotUsed] = Directory
    .ls(path)
    .filter(_.getFileName.toString.endsWith(".csv"))
    .filter(Files.isRegularFile(_))

  def getGraph(
                fileSource: Source[Path, NotUsed],
                fileStats: Sink[Path, Future[Int]],
                measurementStats: Sink[Measurement, Future[(Long, Long)]],
                sensorStats: Sink[SensorStat, Future[Seq[SensorStat]]]
              ): RunnableGraph[(Future[Int], Future[(Long, Long)], Future[Seq[SensorStat]])] =
    RunnableGraph.fromGraph(
      GraphDSL.createGraph(fileStats, measurementStats, sensorStats)((_, _, _)) {
        implicit builder => (fileStats, measurementStats, sensorStats) =>
          import GraphDSL.Implicits._
          val broadcastFiles = builder.add(Broadcast[Path](2))
          val broadcastMeasurements = builder.add(Broadcast[Measurement](2))

          fileSource ~> broadcastFiles
                        broadcastFiles ~> fileStats
                        broadcastFiles ~> fileToContentStringConverter ~> broadcastMeasurements
                                                                          broadcastMeasurements ~> measurementStats
                                                                          broadcastMeasurements ~> sensorStatisticsCalculator ~> sensorStats
          ClosedShape
      })

  type Measurement = (String, Option[Int])
  case class SensorStatValue(count: Long, min: Int, max: Int, avg: Double)
  case class SensorStat(id: String, value: Option[SensorStatValue])
}
