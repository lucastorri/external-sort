package com.github.lucastorri.extsort

import java.io._
import java.nio.charset.Charset
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}

import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

trait ExternalSort {

  def sort(file: Path): Path

}

/**
  * Splits the file in smaller ones, sorting each one individually, and then merge-sorting
  * all intermediate files till a single one is left. Therefore, operations are all done
  * using the disk.
  *
  * @param splitter responsible for breaking the file in smaller blocks.
  * @param mergeGroupSize how many individual files are merged together at once. Must be
  *   at least 2.
  */
case class SplitMergeExternalSort(splitter: FileSplitter, mergeGroupSize: Int = 2) extends ExternalSort with StrictLogging {

  assert(mergeGroupSize > 1, "Merge group size must be bigger than 2")

  override def sort(file: Path): Path = {
    logger.info(s"Sorting $file")
    val output = {
      val name = file.getFileName.toString
      file.getParent.resolve(s"$name.sorted")
    }
    logger.debug(s"Output will be saved to $output")

    logger.trace(s"Splitting file")
    val temps = splitter.split(file).par.map { split =>
      val id = s"[${split.start}-${split.end}]"
      logger.debug(s"Sorting split $id")
      val temp = sort(split)
      logger.trace(s"Sorted split $id to $temp")
      temp
    }

    var merged = temps.seq
    while (merged.size > 1) {
      logger.trace(s"Merges left: ${merged.size}")
      merged = merged.grouped(mergeGroupSize).toSeq.par.map(merge).seq
    }

    logger.info(s"Moving result to output $output")
    Files.move(merged.head, output, StandardCopyOption.REPLACE_EXISTING)

    output
  }

  def sort(split: Split): Path = {
    val sorted = split.lines.sorted.mkString("\n")
    val temp = Files.createTempFile("sorted-", s"-${split.start}-${split.end}")
    Files.write(temp, sorted.getBytes(splitter.charset), StandardOpenOption.CREATE)
    temp
  }

  def merge(files: Seq[Path]): Path = {

    if (files.size == 1) {
      return files.head
    }

    val groupId = files.hashCode
    logger.debug(s"Merging ${files.size} files in $groupId")
    val merged = Files.createTempFile("merged-", "")
    val queue = mutable.PriorityQueue.empty[Item]
    val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(merged.toFile), splitter.charset))

    files.foreach { temp =>
      val reader = LineReader(temp, splitter.charset)
      reader.next().foreach { line =>
        queue.enqueue(Item(line, reader))
      }
    }

    while (queue.nonEmpty) {
      val head = queue.dequeue()
      val next = head.line

      out.write(next.toCharArray)
      if (next.isEmpty || next.last != '\n') {
        out.write('\n')
      }

      head.reader.next().foreach { line =>
        queue.enqueue(head.copy(line = line))
      }
    }

    out.flush()
    out.close()

    logger.debug(s"Merged $groupId")

    merged
  }

}

/**
  * Lazily read each line of the file.
  *
  * @param file the file to read
  * @param charset the file's charset
  */
case class LineReader(file: Path, charset: Charset) {

  private val reader = new BufferedReader(
    new InputStreamReader(
      new FileInputStream(file.toFile), charset), 4 * 1024)

  def next(): Option[String] = {
    val buf = mutable.StringBuilder.newBuilder
    var notEnded = true
    while (notEnded) {
      reader.read() match {
        case -1 =>
          notEnded = false
        case '\n' =>
          notEnded = false
          buf.append('\n')
        case c =>
          buf.append(c.toChar)
      }
    }
    if (buf.isEmpty) {
      reader.close()
      None
    } else {
      Some(buf.toString())
    }
  }

  def toStream: Stream[String] = {
    Stream.continually(next()).takeWhile(_.isDefined).flatten
  }

}

case class Item(line: String, reader: LineReader) extends Ordered[Item] {

  override def compare(that: Item): Int = that.line.compare(line)

}
