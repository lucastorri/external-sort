package com.github.lucastorri.extsort

import org.scalatest.{FlatSpec, MustMatchers}

class ParallelFileSplitterTest extends FlatSpec with MustMatchers with TestInput {

  it must "split files" in {
    val splits = ParallelFileSplitter(1, charset).split(file)

    val text = splits.map(s => s.lines.mkString(",")).toIndexedSeq

    text.size must equal (99)
    text.last must equal ("98,99")
    (0 until 98).foreach { i =>
      text(i).toInt must equal (i)
    }
  }

}
