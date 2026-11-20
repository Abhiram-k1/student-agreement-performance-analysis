package edu.bigdata

import java.nio.file.Paths
import org.apache.spark.{SparkConf, SparkContext}

/** Integration checks compare distributed operations against independent local calculations. */
object StudentAnalysisChecks {
  def main(args: Array[String]): Unit = {
    val path = if (args.nonEmpty) Paths.get(args(0)) else Paths.get("data/sample/03_Dataset.csv")
    val records = StudentLoader.load(path)
    val context = new SparkContext(new SparkConf().setAppName("Assignment 1 integration checks").setMaster("local[2]").set("spark.ui.enabled", "false"))
    context.setLogLevel("WARN")
    try {
      val result = StudentAnalysis.analyze(context.parallelize(records, 3))
      assert(result.recordCount == records.size)
      assert(result.uniqueStudentCount == records.map(_.studentId).distinct.size)
      val expectedAverage = records.map(_.marks).sum / records.size
      assert(math.abs(result.averageMarks - expectedAverage) < 1e-9)
      assert(result.highestMarks == records.map(_.marks).max)
      assert(result.highestStudents.map(s => (s.studentId, s.semester)).toSet ==
        records.filter(_.marks == result.highestMarks).map(s => (s.studentId, s.semester)).toSet)
      assert(result.above75.map(s => (s.studentId, s.semester)).toSet ==
        records.filter(_.marks > 75).map(s => (s.studentId, s.semester)).toSet)
      val expectedCounts = records.groupBy(_.department).map { case (department, students) =>
        department -> students.map(_.studentId).distinct.size.toLong
      }
      assert(result.departmentCounts.toMap == expectedCounts)
      records.groupBy(_.department).foreach { case (department, students) =>
        assert(math.abs(result.departmentAverages.toMap.apply(department) - students.map(_.marks).sum / students.size) < 1e-9)
      }
      try {
        StudentAnalysis.analyze(context.emptyRDD[Student])
        throw new AssertionError("Empty dataset should fail")
      } catch { case _: IllegalArgumentException => () }
      println(s"All distributed integration checks passed on ${records.size} genuine source-derived records.")
    } finally context.stop()
  }
}
