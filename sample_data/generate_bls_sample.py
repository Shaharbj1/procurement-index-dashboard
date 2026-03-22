"""
Run once to generate sample_bls.xlsx in the same folder.
Usage: python sample_data/generate_bls_sample.py
"""
import os
import openpyxl

rows = [
    ("WPU061", 2023, "M01", 245.8),
    ("WPU061", 2023, "M02", 247.1),
    ("WPU061", 2023, "M03", 248.6),
    ("WPU061", 2023, "M04", 249.3),
    ("WPU061", 2023, "M05", 250.0),
    ("WPU061", 2023, "M06", 251.2),
    ("WPU061", 2023, "M07", 252.4),
    ("WPU061", 2023, "M08", 253.1),
    ("WPU061", 2023, "M09", 254.0),
    ("WPU061", 2023, "M10", 254.8),
    ("WPU061", 2023, "M11", 255.3),
    ("WPU061", 2023, "M12", 256.0),
    ("WPU061", 2024, "M01", 257.2),
    ("WPU061", 2024, "M02", 258.0),
    ("WPU061", 2024, "M03", 258.9),
    ("WPU061", 2024, "M04", 259.7),
    ("WPU061", 2024, "M05", 260.3),
    ("WPU061", 2024, "M06", 261.1),
]

wb = openpyxl.Workbook()
ws = wb.active
ws.title = "BLS Data"
ws.append(["Series Id", "Year", "Period", "Value", "Footnotes"])
for r in rows:
    ws.append([*r, ""])

out = os.path.join(os.path.dirname(__file__), "sample_bls.xlsx")
wb.save(out)
print(f"Generated: {out}")
