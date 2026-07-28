#!/usr/bin/env python3
"""Build three isolated legacy GAS modules for one central Web App project."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKUP = ROOT / "gas_backup"
OUTPUT = ROOT / "gas_central"

APPS = {
    "payment": ("Payment", BACKUP / "payment" / "Code.gs", BACKUP / "payment" / "Panel.html"),
    "cleaning": ("Cleaning", BACKUP / "cleaning" / "Code.gs", BACKUP / "cleaning" / "Sidebar.html"),
    "other": ("Other", BACKUP / "other" / "Code.gs", BACKUP / "other" / "Panel.html"),
}


def function_names(source: str) -> list[str]:
    return sorted(set(re.findall(r"(?m)^function\s+([A-Za-z_$][\w$]*)\s*\(", source)))


def build_module(app: str, class_name: str, source: str) -> str:
    source = source.replace(
        "SpreadsheetApp.getActiveSpreadsheet()",
        "CentralContext.getSpreadsheet()",
    ).replace(
        "SpreadsheetApp.getActive()",
        "CentralContext.getSpreadsheet()",
    )
    if app == "payment":
        source = source.replace(
            "PeriodManager.getCurrentPeriodFromFilename()",
            "CentralContext.getPeriod()",
        )
        start = source.index("const ExecutionLogger = {")
        end_marker = "\n\n// =========================\n// 主程式入口"
        end = source.index(end_marker, start)
        source = source[:start] + """const ExecutionLogger = {
  recordExecutionLog(period, label, value) {
    return CentralMaster.recordExecution(label, value, period);
  },
  recordExecutionId(label, id) {
    return CentralMaster.recordExecution(label, id, CentralContext.getPeriod());
  },
  recordExecutionLogs(period, map) {
    Object.keys(map).forEach(label =>
      CentralMaster.recordExecution(label, map[label], period)
    );
  },
  getExecutionRowCount(period, label) {
    return CentralMaster.getExecutionValue(label, period);
  }
};""" + source[end:]
    elif app == "cleaning":
        source = source.replace("      execSheet.activate();\n", "")
        source = source.replace("      execSheet.getRange(cellAddress).activate();\n", "")
        source = source.replace(
            "      execSheet.getRange(cellAddress).setValue(timestamp);",
            "      CentralMaster.recordExecution(functionName, null, CentralContext.getPeriod());",
        )
    names = function_names(source)
    exports = ",\n".join(f"    {name}: {name}" for name in names)
    wrappers = "\n".join(
        f"function {app}_{name}() {{ return {class_name}App.{name}.apply(null, arguments); }}"
        for name in names
        if name not in {"doGet", "doPost", "onOpen", "showSidebar"}
    )
    return (
        f"// Generated from gas_backup/{app}. Do not edit directly.\n"
        f"var {class_name}App = (function () {{\n{source}\n\n"
        f"  return {{\n{exports}\n  }};\n"
        f"}})();\n\n{wrappers}\n"
    )


def build_html(app: str, source: str, names: list[str]) -> str:
    if app == "payment":
        source = source.replace(
            "runner[functionName](...args);",
            f'runner["{app}_" + functionName](...args);',
        )
    for name in sorted(names, key=len, reverse=True):
        source = source.replace(f".{name}(", f".{app}_{name}(")
    return source


def main() -> None:
    OUTPUT.mkdir(exist_ok=True)
    for app, (class_name, code_path, html_path) in APPS.items():
        code = code_path.read_text(encoding="utf-8")
        html = html_path.read_text(encoding="utf-8")
        names = function_names(code)
        (OUTPUT / f"{class_name}Module.gs").write_text(
            build_module(app, class_name, code),
            encoding="utf-8",
        )
        (OUTPUT / f"{class_name}Panel.html").write_text(
            build_html(app, html, names),
            encoding="utf-8",
        )

    schedule = (BACKUP / "scheduler" / "ScheduleTrigger.gs").read_text(encoding="utf-8")
    schedule = re.sub(
        r"function doGet\(e\)\s*\{\s*return handleRequest_\(e\);\s*\}\s*",
        "",
        schedule,
    )
    schedule = re.sub(
        r"function doPost\(e\)\s*\{\s*return handleRequest_\(e\);\s*\}\s*",
        "",
        schedule,
    )
    (OUTPUT / "ScheduleTrigger.gs").write_text(schedule, encoding="utf-8")


if __name__ == "__main__":
    main()
