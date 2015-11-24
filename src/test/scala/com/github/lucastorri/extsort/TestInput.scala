package com.github.lucastorri.extsort

import java.nio.charset.StandardCharsets
import java.nio.file.Files

trait TestInput {



  val lines = (0 to 99).map(i => f"$i%02d").mkString("\n")
  val charset = StandardCharsets.UTF_8
  val file = Files.createTempFile("split-", "-test-file")

  Files.write(file, lines.getBytes(charset))

}
