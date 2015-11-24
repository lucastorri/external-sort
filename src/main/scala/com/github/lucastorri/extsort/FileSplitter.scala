package com.github.lucastorri.extsort

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.{ByteBuffer, CharBuffer}

import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

trait FileSplitter {

  def split(f: Path): Seq[Split]

  def charset: Charset

}

case class Split(file: Path, charset: Charset, start: Long, end: Long) {

  def lines: IndexedSeq[String] = {
    val buf = ByteBuffer.allocate(size)
    val fc = FileChannel.open(file, StandardOpenOption.READ)
    fc.read(buf, start)
    fc.close()
    val decoded = new String(buf.array(), charset)
    decoded.toString.split('\n').filter(_.nonEmpty)
  }

  def size: Int = (end - start).toInt

}

/**
  * Creates blocks that have at least the given size. The block will contain the number of requested chars,
  * plus the ones necessary to reach an end of line `\n`.
  *
  * @param charsPerSplit minimum number of characters on the block
  * @param charset the charset to be used
  */
case class OverflowedMaxSizeFileSplitter(charsPerSplit: Long, charset: Charset) extends FileSplitter with StrictLogging {

  private val skipBuffer = Array.ofDim[Char](8 * 1024)
  private val charBuffer = CharBuffer.allocate(1)

  override def split(file: Path): Seq[Split] = {
    val fileSize = Files.size(file)
    logger.debug(s"Splitting $file with size $fileSize")
    val splits = mutable.ListBuffer.empty[Split]
    val reader = new InputStreamReader(new FileInputStream(file.toFile), charset)

    var start = 0L
    var notFinished = true
    var bytesRead = 0L

    def read(n: Long = 1L): Int = {

      var left = n
      var got = 0

      while (left > 0 && bytesRead < fileSize) {
        val readIntoBuffer = math.min(if (left > Int.MaxValue) Int.MaxValue else left.toInt, skipBuffer.length)
        got = reader.read(skipBuffer, 0, readIntoBuffer)
        (0 until got).foreach(i => bytesRead += charSizeInBytes(skipBuffer(i)))
        left -= got
      }

      skipBuffer.lift(got - 1).map(_.toInt).getOrElse(-1)
    }

    read(charsPerSplit)
    while (notFinished) {
      read() match {
        case -1 =>
          notFinished = false
          logger.trace(s"Split at $start-$bytesRead")
          splits.append(Split(file, charset, start, bytesRead))
        case '\n' =>
          logger.trace(s"Split at $start-$bytesRead")
          splits.append(Split(file, charset, start, bytesRead))
          start = bytesRead
          read(charsPerSplit)
        case c =>
      }
    }

    reader.close()
    splits.toSeq
  }

  def charSizeInBytes(c: Char): Int = {
    charBuffer.position(0)
    charBuffer.append(c)
    charBuffer.position(0)
    charset.encode(charBuffer).array().length
  }

}

case class ParallelFileSplitter(bytesPerSplit: Long, charset: Charset) extends FileSplitter with StrictLogging {

  override def split(file: Path): Seq[Split] = {
    val fileSize = Files.size(file)
    logger.debug(s"Splitting $file with size $fileSize")

    var needsToSplit = true
    var splits = Seq(0L -> fileSize).par

    while (needsToSplit) {
      var wasSplit = false
      splits = splits.flatMap { case (start, end) =>
        val n = split(file, start, end)
        wasSplit |= n.size > 1
        n
      }
      needsToSplit = wasSplit
    }

    logger.info("Split done")
    splits.map { case (start, end) => Split(file, charset, start, end) }.seq
  }

  private def split(file: Path, start: Long, end: Long): Seq[(Long, Long)] = {
    if ((end - start) <= bytesPerSplit) {
      return Seq(start -> end)
    }

    val middle = (end + start) / 2
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile), charset))

    var eol = middle
    def read(): Int = {
      val next = reader.read()
      if (next > 0) eol += charSizeInBytes(next.toChar)
      next
    }

    while (eol < end && read() != '\n') {}
    reader.close()

    val split =
      if (eol >= end) Seq(start -> end)
      else Seq(start -> eol, eol -> end)

    logger.debug {
      val str = split.map { case (s, e) => s"$s-$e"}.mkString("[", ",", "]")
      s"Splitting $start-$end into $str"
    }
    split
  }

  def charSizeInBytes(c: Char): Int = {
    val charBuffer = CharBuffer.allocate(1)
    charBuffer.position(0)
    charBuffer.append(c)
    charBuffer.position(0)
    charset.encode(charBuffer).array().length
  }

}
