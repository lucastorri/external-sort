package com.github.lucastorri.extsort

import java.io.{FileInputStream, InputStreamReader}
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.{ByteBuffer, CharBuffer}

import com.google.common.io.CountingInputStream
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
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
