"""PDF reporting utilities for BTC data."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


class ReportMaker:
    """Helper to assemble multi-section PDF reports."""

    def __init__(self, report_dir: str | Path, title: str) -> None:
        report_dir = Path(report_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        self.doc = SimpleDocTemplate(
            str(report_dir / f"{title}.pdf"),
            pagesize=LETTER,
            rightMargin=40,
            leftMargin=40,
            topMargin=40,
            bottomMargin=40,
        )
        self.styles = getSampleStyleSheet()
        self.flow = []

    def add_section(self, title: str) -> None:
        self.flow.append(Paragraph(title, self.styles["Title"]))
        self.flow.append(Spacer(1, 0.15 * inch))

    def add_table(self, table: pd.DataFrame) -> None:
        data = [table.columns.tolist()] + table.values.tolist()
        report_table = Table(data, hAlign="LEFT")
        report_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                ]
            )
        )
        self.flow.append(report_table)
        self.flow.append(Spacer(1, 0.1 * inch))

    def add_paragraph(self, text: str) -> None:
        self.flow.append(Paragraph(text, self.styles["Normal"]))
        self.flow.append(Spacer(1, 0.1 * inch))

    def add_image(
        self,
        image_source: str | Path,
        width: Optional[float] = None,
        height: Optional[float] = None,
        add_page_break: bool = False,
    ) -> None:
        img = Image(image_source)
        if width and height:
            img.drawWidth = width
            img.drawHeight = height
        elif width:
            img.drawWidth = width
            img.drawHeight = width * img.imageHeight / img.imageWidth
        self.flow.append(img)
        self.flow.append(Spacer(1, 0.1 * inch))
        if add_page_break:
            self.flow.append(PageBreak())

    def save(self) -> None:
        self.doc.build(self.flow)
