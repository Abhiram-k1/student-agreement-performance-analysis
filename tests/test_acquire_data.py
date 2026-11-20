"""Test source adaptation invariants against genuine downloaded rows."""
import csv
import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("acquire_data", ROOT / "scripts/acquire_data.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class AdaptationChecks(unittest.TestCase):
    def test_adaptation_matches_source(self):
        source = ROOT / "data/raw/uci_students.csv"
        self.assertTrue(source.is_file(), "Run python scripts/acquire_data.py before this test")
        records, total, omitted = MODULE.adapt(source.read_bytes(), 1)
        with source.open(encoding="utf-8-sig", newline="") as handle:
            original = list(csv.DictReader(handle, delimiter=";"))
        self.assertEqual(total, len(original))
        self.assertEqual(len(records) + omitted, total)
        for record in records:
            number = int(record["StudentID"].split("-")[1])
            row = original[number - 1]
            self.assertEqual(record["Department"], "Course_" + row["Course"])
            self.assertAlmostEqual(float(record["Marks"]), float(row["Curricular units 1st sem (grade)"]) * 5, places=7)
            self.assertGreater(int(row["Curricular units 1st sem (evaluations)"]), 0)


if __name__ == "__main__":
    unittest.main()
