"""
PDF Generator for Company Reports
Generates formatted PDF reports using ReportLab with exact specifications.
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor, white
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from typing import Dict, List, Any
import os


TITLE_COLOR = HexColor("#1a365d")
SECTION_HEADING_COLOR = HexColor("#2c5282")
SUBHEADING_COLOR = HexColor("#4a5568")
TABLE_HEADER_BG = HexColor("#2c5282")
ALT_ROW_COLOR = HexColor("#f7fafc")
GREY_TEXT = HexColor("#718096")


def create_styles() -> Dict[str, ParagraphStyle]:
    """Create custom paragraph styles matching the specification."""
    base_styles = getSampleStyleSheet()
    
    styles = {
        "title": ParagraphStyle(
            "CustomTitle",
            parent=base_styles["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=18,
            textColor=TITLE_COLOR,
            alignment=TA_CENTER,
            spaceAfter=20,
        ),
        "section_heading": ParagraphStyle(
            "SectionHeading",
            parent=base_styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=14,
            textColor=SECTION_HEADING_COLOR,
            spaceBefore=15,
            spaceAfter=10,
        ),
        "subheading": ParagraphStyle(
            "Subheading",
            parent=base_styles["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=12,
            textColor=SUBHEADING_COLOR,
            spaceBefore=10,
            spaceAfter=6,
        ),
        "body": ParagraphStyle(
            "CustomBody",
            parent=base_styles["Normal"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            spaceAfter=6,
        ),
        "source": ParagraphStyle(
            "Source",
            parent=base_styles["Normal"],
            fontName="Helvetica",
            fontSize=8,
            textColor=GREY_TEXT,
            alignment=TA_CENTER,
            spaceBefore=20,
        ),
        "bullet": ParagraphStyle(
            "Bullet",
            parent=base_styles["Normal"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            leftIndent=20,
            spaceAfter=4,
        ),
    }
    
    return styles


def create_two_column_table(data: List[List[str]], col_widths: List[float] = None) -> Table:
    """Create a two-column table for company profile data."""
    if col_widths is None:
        col_widths = [2.5 * inch, 4.5 * inch]
    
    table = Table(data, colWidths=col_widths)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), ALT_ROW_COLOR),
        ("TEXTCOLOR", (0, 0), (-1, -1), HexColor("#2d3748")),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("ALIGN", (1, 0), (1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#e2e8f0")),
    ]))
    
    return table


def create_three_column_table(headers: List[str], data: List[List[str]]) -> Table:
    """Create a three-column table with alternating row colors."""
    all_data = [headers] + data
    col_widths = [2.5 * inch, 2 * inch, 2.5 * inch]
    
    table = Table(all_data, colWidths=col_widths)
    
    style_commands = [
        ("BACKGROUND", (0, 0), (-1, 0), TABLE_HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#e2e8f0")),
    ]
    
    for i in range(1, len(all_data)):
        if i % 2 == 0:
            style_commands.append(("BACKGROUND", (0, i), (-1, i), ALT_ROW_COLOR))
        else:
            style_commands.append(("BACKGROUND", (0, i), (-1, i), white))
    
    table.setStyle(TableStyle(style_commands))
    
    return table


def create_charges_table(charges_data: Dict[str, Any]) -> Table:
    """Create a table for charges information."""
    data = [
        ["Charge Type", "Count"],
        ["Outstanding", str(charges_data.get("outstanding", 0))],
        ["Satisfied", str(charges_data.get("satisfied", 0))],
        ["Total", str(charges_data.get("total_charges", 0))],
    ]
    
    col_widths = [3.5 * inch, 3.5 * inch]
    table = Table(data, colWidths=col_widths)
    
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), TABLE_HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (1, 0), (1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#e2e8f0")),
        ("BACKGROUND", (0, 1), (-1, 1), ALT_ROW_COLOR),
        ("BACKGROUND", (0, 3), (-1, 3), ALT_ROW_COLOR),
    ]))
    
    return table


def generate_pdf(company_data: Dict[str, Any], output_path: str) -> str:
    """Generate a PDF report from company data."""
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )
    
    styles = create_styles()
    story = []
    
    profile = company_data.get("profile", {})
    company_name = profile.get("registered_name", "Unknown Company")
    
    story.append(Paragraph(f"Corporate Onboarding Report: {company_name}", styles["title"]))
    story.append(Spacer(1, 10))
    
    story.append(Paragraph("1. Basic Company Profile", styles["section_heading"]))
    
    profile_data = [
        ["Registered Name", profile.get("registered_name", "N/A")],
        ["Company Number", profile.get("company_number", "N/A")],
        ["Registered Office", profile.get("registered_office_address", "N/A")],
        ["Company Status", profile.get("company_status", "N/A")],
        ["Company Type", profile.get("company_type", "N/A")],
        ["Incorporated", profile.get("incorporation_date", "N/A")],
        ["Country of Origin", profile.get("country_of_origin", "United Kingdom")],
    ]
    
    story.append(create_two_column_table(profile_data))
    story.append(Spacer(1, 10))
    
    sic_codes = profile.get("sic_codes", [])
    if sic_codes:
        story.append(Paragraph("SIC Codes (Nature of Business):", styles["subheading"]))
        for sic in sic_codes:
            story.append(Paragraph(f"- {sic}", styles["bullet"]))
    
    story.append(Spacer(1, 10))
    
    story.append(Paragraph("2. Directors Overview", styles["section_heading"]))
    
    officers = company_data.get("officers", [])
    if officers:
        officers_data = []
        for officer in officers[:10]:
            officers_data.append([
                officer.get("name", "N/A"),
                officer.get("role", "N/A"),
                officer.get("appointed_date", "N/A"),
            ])
        
        story.append(create_three_column_table(
            ["Name", "Role", "Appointed"],
            officers_data
        ))
    else:
        story.append(Paragraph("No officer information available.", styles["body"]))
    
    story.append(Spacer(1, 10))
    
    story.append(Paragraph("3. Recent Filings and Notable Events", styles["section_heading"]))
    
    filings = company_data.get("filings", [])
    if filings:
        story.append(Paragraph("Recent Filing History:", styles["subheading"]))
        for filing in filings[:8]:
            date = filing.get("date", "N/A")
            desc = filing.get("description", "N/A")
            story.append(Paragraph(f"- {date}: {desc}", styles["bullet"]))
    else:
        story.append(Paragraph("No recent filings available.", styles["body"]))
    
    story.append(Spacer(1, 10))
    
    charges = company_data.get("charges", {})
    story.append(Paragraph("Charges Summary:", styles["subheading"]))
    story.append(create_charges_table(charges))
    
    story.append(Spacer(1, 10))
    
    story.append(Paragraph("4. Red Flags and Follow-up Checks", styles["section_heading"]))
    
    red_flags = [
        "Verify current director appointments against official records",
        "Review any outstanding charges for potential financial risk",
        "Confirm registered office address is current and accessible",
        "Check for any recent significant corporate actions or restructuring",
        "Verify company status is Active with no pending dissolution",
        "Review filing history for compliance with statutory requirements",
        "Cross-reference SIC codes with actual business activities",
    ]
    
    for flag in red_flags:
        story.append(Paragraph(f"- {flag}", styles["bullet"]))
    
    story.append(Spacer(1, 20))
    
    collection_date = company_data.get("collection_date", "Unknown")
    source_text = (
        f"Data sourced from UK Companies House (find-and-update.company-information.service.gov.uk) "
        f"on {collection_date}. This report is for informational purposes only. "
        f"Please verify all information directly with Companies House for official records."
    )
    story.append(Paragraph(source_text, styles["source"]))
    
    doc.build(story)
    
    return output_path


if __name__ == "__main__":
    sample_data = {
        "profile": {
            "registered_name": "TESCO PLC",
            "company_number": "00445790",
            "registered_office_address": "Tesco House, Shire Park, Kestrel Way, Welwyn Garden City, AL7 1GA",
            "company_status": "Active",
            "company_type": "Public limited Company",
            "incorporation_date": "27 November 1947",
            "country_of_origin": "United Kingdom",
            "sic_codes": ["47110 - Retail sale in non-specialised stores with food, beverages or tobacco predominating"]
        },
        "officers": [
            {"name": "John Smith", "role": "Director", "appointed_date": "1 January 2020"},
            {"name": "Jane Doe", "role": "Director", "appointed_date": "15 March 2019"},
        ],
        "filings": [
            {"date": "22 Jan 2026", "description": "Annual accounts"},
            {"date": "08 Jan 2026", "description": "Confirmation statement"},
        ],
        "charges": {
            "total_charges": 9,
            "outstanding": 2,
            "satisfied": 7,
        },
        "collection_date": "2026-01-26 12:00:00"
    }
    
    output = generate_pdf(sample_data, "test_report.pdf")
    print(f"PDF generated: {output}")
