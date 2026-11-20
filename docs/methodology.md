# Methodology and project explanation

## Input preparation

The downloader obtains the original UCI archive, extracts the original CSV bytes and saves them unchanged. It reads genuine selected-semester grades, removes records without evaluations, converts measured grades to a percentage scale and creates stable anonymous display labels. It writes a provenance record containing a source checksum and actual retained/omitted counts.

`StudentLoader` validates the required five-column schema and creates a Scala `Vector[Student]`. This directly satisfies the required entity and object-collection step. The driver calls `SparkContext.parallelize` with a configurable partition count.

## Required analyses

`filter` selects records with marks strictly greater than 75. `map` projects each record into the fields or key/value pairs needed by later calculations. Department counts first remove duplicate `(department, studentId)` pairs and then use `reduceByKey`. The overall average uses a distributed sum/count reduction. The maximum is computed from the marks RDD, then the student RDD is filtered for every record tied for that maximum.

The small assignment dataset supports collecting complete result tables to the driver and displaying every record. The implementation deliberately fulfills the required all-record display. A very large dataset would need a different result presentation and acquisition design to avoid driver collection limits.

## Transformations, actions and lazy evaluation

Transformations such as `filter`, `map`, `distinct`, `reduceByKey` and `sortByKey` create new RDDs with dependency information. They do not immediately process every record. Actions such as `collect`, `count`, `reduce` and `max` request results and trigger jobs.

The main application constructs a chain of several transformations before an action. A Spark listener counts actual job-start events. The program checks that transformation construction leaves the count unchanged and that the first `collect` increases it. The recorded counts and pre-action `toDebugString` provide reproducible lazy-evaluation evidence.

The demonstration uses filter, map and reduceByKey. The reporting operations also sort records. Spark's range-based sorting can sample data while constructing its partitioner, so that sorting is performed outside the chain used for the no-job construction check.

The student RDD uses `MEMORY_ONLY` persistence. The first action evaluates and caches its partitions. Later actions can reuse cached partitions while still executing their own downstream processing. Persistence is a lazy request and materializes during an action.

## DAG, stages and task distribution

Spark constructs a dependency DAG from the RDD lineage. Narrow dependencies such as map/filter allow a partition to be processed along a chain within one task. Wide dependencies such as distinct, reduceByKey and sortByKey can require shuffles and stage boundaries. A shuffle redistributes records by keys or partitioning rules across executor processes.

The driver schedules jobs and stages. For a stage, Spark typically launches one task per partition to be processed. Executors run those tasks in parallel according to available cores and resources. `local[2]` uses two execution threads in one machine. A Spark standalone cluster places tasks on registered worker executors, with retries handled by Spark if a task fails.

`partition_sizes.csv` reports the actual input-partition row counts. The Spark UI Jobs, Stages and Executors pages show the task/stage/executor information from the run. Saved event logs let the History Server recreate that evidence after the application stops. `rdd_lineage.txt` is dependency evidence and the UI supplies the rendered generated DAG.

## Evaluation

This assignment specifies deterministic data-analysis operations. Validation compares distributed results with independent local calculations on the genuine test subset. It checks threshold semantics, counts, averages, maximum ties and clear handling of an empty RDD. The acquisition test verifies retained source values and the 20-to-100 grade conversion. The run metadata records actual runtime observations. See `validation.md` for the tests actually run during packaging.

Interpret results in the context of the selected semester and source course-code grouping. The average excludes students without evaluations, so it describes observed academic performance among evaluated records. It does not estimate unobserved grades.
