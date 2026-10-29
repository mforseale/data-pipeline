"""Execute real tests in the container and retain a machine-readable summary."""
import json
from pathlib import Path
import unittest

root = Path(__file__).resolve().parents[1]
suite = unittest.defaultTestLoader.discover(str(root / "tests"))
result = unittest.TextTestRunner(verbosity=2).run(suite)
summary = {"tests_run": result.testsRun, "failures": len(result.failures),
           "errors": len(result.errors), "skipped": len(result.skipped),
           "success": result.wasSuccessful()}
(root / "results/unit_tests.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
raise SystemExit(0 if result.wasSuccessful() else 1)
