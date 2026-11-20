# Examine the generated Spark DAG and distribution evidence

## Live Spark UI

Run the application with a UI waiting period.

```bash
sbt "run --input data/sample/03_Dataset.csv --ui-wait-seconds 300"
```

Open the actual Spark UI URL printed in the terminal, normally `http://localhost:4040`. The context stays alive for the selected number of seconds after results are saved.

1. Open **Jobs** and inspect the jobs created by `collect`, `count`, `reduce` and `max`.
2. Open a job's details and expand **DAG Visualization**. Observe stage boundaries around wide operations.
3. Open **Stages** and inspect task counts, shuffle read/write and durations.
4. Open **Executors** and examine cores, active/completed tasks and storage.
5. Save any screenshots needed for your explanation in the run directory. Label the input file, master and run timestamp alongside them.

Use `rdd_lineage.txt` to connect the transformations in the source with the observed DAG. Compare `partition_sizes.csv` with task counts. A stage can use a different partitioning from the original RDD after shuffle operations, so input partition count alone does not determine every stage's task count.

## Spark History Server

Install a compatible Spark 3.5.3 distribution and point its History Server at the generated event-log directory. Spark history configuration uses a filesystem URI. For example, on Linux replace the path below with the actual absolute run path.

```bash
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:/absolute/path/to/Assignment_1/outputs/runs/RUN_TIMESTAMP/event_logs"
$SPARK_HOME/sbin/start-history-server.sh
```

Open `http://localhost:18080`. On another platform, set the corresponding environment variable and use the launch procedure supported by that Spark installation. Spark event logs are written and closed by the application when its context stops.

## Cluster evidence

Submit the packaged JAR to your available Spark cluster as described in the README. Inspect the Executors and Stages pages from that run. Explain how the driver assigned partition tasks to executor processes on the workers. Record the real worker/executor count and master URL from your run.

Local execution provides actual Spark job, task, partition, shuffle and DAG evidence. Cluster deployment requires the user's available worker infrastructure and can be documented with that run's observations.
