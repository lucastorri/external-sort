package com.github.lucastorri.extsort

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.{MustMatchers, FlatSpec}

import scala.util.Random

class SplitMergeExternalSortTest extends FlatSpec with MustMatchers {

  val charset = StandardCharsets.UTF_8
  val sort = SplitMergeExternalSort(OverflowedMaxSizeFileSplitter(0, charset))

  it must "sort a single split" in {
    val lines = Random.shuffle(0 to 9).mkString("\n")
    val file = Files.write(Files.createTempFile("sort-", "-test-file"), lines.getBytes(charset))

    val sorted = sort.sort(Split(file, charset, 0, lines.length))
    val sortedString = new String(Files.readAllBytes(sorted), charset)

    sortedString must equal ((0 to 9).mkString("\n"))
  }

  it must "merge files" in {
    val evenLines = (0 to 8 by 2).mkString("", "\n", "\n")
    val oddLines = (1 to 9 by 2).mkString("", "\n", "\n")

    val fileEven = Files.write(Files.createTempFile("merge-", "-test-file"), evenLines.getBytes(charset))
    val fileOdd = Files.write(Files.createTempFile("merge-", "-test-file"), oddLines.getBytes(charset))

    val merged = sort.merge(Seq(fileEven, fileOdd))
    val mergedString = new String(Files.readAllBytes(merged), charset)

    mergedString must equal ((0 to 9).mkString("", "\n", "\n"))
  }

}
