# Assumptions of the current implementation

- garbage collection not a problem, otherwise we would switch to mutable accumulators and do away with case class wrappers
- all files are on the same standard partition where there are no benefits of reading files in parallel, otherwise, if the underlying file system makes it worthwhile, we can read files in parallel.
- double precision and long sized number of observations fit the context, otherwise we switch to Big{Integer, Decimal}

# Usage

In `sbt` shell use `run --data-files /pathToYourFiles`.
