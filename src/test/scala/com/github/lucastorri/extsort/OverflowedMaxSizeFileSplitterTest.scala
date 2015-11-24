package com.github.lucastorri.extsort

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.{MustMatchers, FlatSpec}

class OverflowedMaxSizeFileSplitterTest extends FlatSpec with MustMatchers with TestInput {

  it must "split lines" in {

    val splits = OverflowedMaxSizeFileSplitter(0, charset).split(file)

    splits.nonEmpty must be (true)
    splits.zipWithIndex.foreach { case (split, i) =>
      val stringSegment = lines.substring(split.start.toInt, split.end.toInt).trim
      val fileSegment = split.lines.mkString

      fileSegment must equal (stringSegment)
      fileSegment.trim.toInt must equal (i)
    }

  }

}
