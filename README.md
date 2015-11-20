# External Sort

<https://en.wikipedia.org/wiki/External_sorting>

Implementation of external sort using Scala. It receives a file (so far tested with 3GB files), where content is separated by lines (`\n`).

The given file is split in smaller parts, each is then individually sorted in parallel, and all smaller parts are merged using a priority queue, till they are merged on a single file.

I tried to use `nio` as much as possible. The program is charset aware, meaning it will respect characters that need more than 1 byte to be represented.

Since in certain charsets, character sizes are variable, one of the problems encountered was how to translate the number of chars read to the number of bytes read. That was manually implemented on the `OverflowedMaxSizeFileSplitterTest` class.

