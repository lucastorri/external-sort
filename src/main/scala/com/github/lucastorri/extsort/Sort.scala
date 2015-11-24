package com.github.lucastorri.extsort

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.util.Random

object Settings {
  val file = {
    val home = System.getProperty("user.home")
    Paths.get(home).resolve("tmp/big-file")
  }
  val charset = StandardCharsets.UTF_8
}

object Sort extends App with StrictLogging {

  val splitter = ParallelFileSplitter(16 * 1024 * 1024, Settings.charset)
  val sorter = SplitMergeExternalSort(splitter, 4)

  def timed[R](action: => R): (R, Long) = {
    val t0 = System.currentTimeMillis()
    val result = action
    val t1 = System.currentTimeMillis()
    result -> (t1 - t0)
  }

  val (sorted, time) = timed {
    sorter.sort(Settings.file)
  }

  logger.info(s"Sorted file: $sorted")
  logger.info(s"Total time: $time ms")

}

object GenerateTestFile extends App {

  val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Settings.file.toFile), Settings.charset))

  Stream.range(0, Int.MaxValue).grouped(2048).foreach { set =>
    val binaryStrings = set.par.map(i => f"${Integer.toBinaryString(i)}%32s".replace(' ', '0')).seq
    val rand = Random.shuffle(binaryStrings)
    val str = rand.mkString("", "\n", "\n")
    out.write(str.toCharArray)
  }

  out.flush()
  out.close()

}

object ValidateSortedFile extends App with StrictLogging {

  logger.info(s"Validating file: ${Settings.file}")

  val stream = LineReader(Paths.get(s"${Settings.file}.sorted"), Settings.charset).toStream

  @tailrec
  def invalidPair(pairs: Stream[String]): Option[(String, String)] = {
    if (pairs.size < 2) None
    else {
      val tail = pairs.tail
      val n0 = pairs.head
      val n1 = tail.head
      if (n0.compare(n1) > 0) Some(n0 -> n1)
      else invalidPair(tail)
    }
  }

  invalidPair(stream) match {
    case Some((l0, l1)) => logger.info(s"File is not sorted:\n  * $l0  * $l1")
    case None => logger.info("File is sorted")
  }

}
