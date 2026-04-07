from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

from openpyxl import Workbook


def _autosize_columns(worksheet) -> None:
    for column_cells in worksheet.columns:
        values = [len(str(cell.value or "")) for cell in column_cells]
        worksheet.column_dimensions[column_cells[0].column_letter].width = min(
            max(values + [12]) + 2,
            60,
        )


def _build_workbook(title: str, headers: list[str], rows: Iterable[Iterable[object]]) -> Workbook:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = title
    worksheet.append(headers)
    for row in rows:
        worksheet.append(list(row))
    _autosize_columns(worksheet)
    worksheet.freeze_panes = "A2"
    return workbook


def export_activation_history_xlsx(path: Path, rows: Iterable[Iterable[object]]) -> Path:
    workbook = _build_workbook(
        "Activations",
        [
            "Order ID",
            "Status",
            "Telegram ID",
            "Telegram Name",
            "Username",
            "Email",
            "Activation Code",
            "App",
            "Product",
            "Created At",
            "Updated At",
            "Task ID",
        ],
        rows,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    return path


def export_users_xlsx(path: Path, rows: Iterable[Iterable[object]]) -> Path:
    workbook = _build_workbook(
        "Users",
        [
            "Telegram ID",
            "Telegram Name",
            "Username",
            "Is Admin",
            "First Seen",
            "Last Seen",
        ],
        rows,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    return path


def build_export_path(base_dir: Path, prefix: str) -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    return base_dir / f"{prefix}_{timestamp}.xlsx"
