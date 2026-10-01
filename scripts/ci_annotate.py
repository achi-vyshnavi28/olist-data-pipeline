"""Turn failing tests in a JUnit XML report into GitHub Actions error annotations, so the failure reason shows on the
run summary page without opening the full log.

    python scripts/ci_annotate.py reports/junit.xml
"""
import sys
import xml.etree.ElementTree as ET


def main(path: str) -> int:
    failed = 0
    for case in ET.parse(path).getroot().iter("testcase"):
        for kind in ("failure", "error"):
            node = case.find(kind)
            if node is None:
                continue
            failed += 1
            detail = (node.get("message") or "") + " | " + (node.text or "").strip().splitlines()[-1:][0] if (node.text or "").strip() else (node.get("message") or "")
            detail = detail.replace("%", "%25").replace("\r", "").replace("\n", " ")[:900]
            print(f"::error title={case.get('classname')}.{case.get('name')}::{detail}")
    return failed


if __name__ == "__main__":
    print(f"{main(sys.argv[1])} failing tests annotated")
