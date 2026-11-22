"""Build the submission PDF from project_report.md.

The figures are read off disk and inlined as base64 data URIs, so the PDF
carries the images themselves. An earlier PDF was produced by a tool outside
this repository and shipped the figures as literal markdown text, which left
every graph invisible to anyone the file was shared with.

Chain:  project_report.md  ->  self-contained HTML  ->  headless Edge  ->  PDF

The course template fixes the page: A4, 2.5 cm margins, 10 pt body, 6 to 8
pages. Page count is reported after the build; it is not estimated.

Usage (PowerShell or bash, from anywhere):
    python etl4/docs/build_report.py
    python etl4/docs/build_report.py --out some/other.pdf --keep-html
"""
from __future__ import annotations

import argparse
import base64
import html
import mimetypes
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DOCS = Path(__file__).resolve().parent
MD = DOCS / "project_report.md"
OUT = DOCS / "ETL4_Final_Project_Report.pdf"

EDGE_CANDIDATES = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]

CSS = """
@page { size: A4; margin: 25mm; }
html { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
body {
  font-family: "Times New Roman", Times, serif;
  font-size: 10pt; line-height: 1.21; color: #000; margin: 0;
  text-align: justify; hyphens: auto;
}
h1 { font-size: 14pt; line-height: 1.18; margin: 0 0 3pt 0; text-align: left; }
h2 { font-size: 11.5pt; margin: 8pt 0 2pt 0; text-align: left;
     break-after: avoid; page-break-after: avoid; }
h3 { font-size: 10pt; margin: 6pt 0 1pt 0; text-align: left;
     break-after: avoid; page-break-after: avoid; }
p  { margin: 0 0 3pt 0; }
blockquote { margin: 3pt 0 3pt 14pt; font-style: italic; }
ol, ul { margin: 0 0 3pt 0; padding-left: 15pt; }
li { margin: 0 0 1pt 0; }
hr { border: 0; border-top: 0.5pt solid #999; margin: 5pt 0; }
code { font-family: Consolas, "Courier New", monospace; font-size: 8.8pt; }
pre {
  font-family: Consolas, "Courier New", monospace; font-size: 6.8pt;
  line-height: 1.14; background: #f6f7f8; border: 0.5pt solid #d8dce1;
  padding: 4pt 6pt; margin: 0 0 4pt 0; white-space: pre; overflow: hidden;
  break-inside: avoid; page-break-inside: avoid; text-align: left;
}
table {
  border-collapse: collapse; width: 100%; font-size: 8.1pt;
  margin: 0 0 4pt 0; break-inside: avoid; page-break-inside: avoid;
}
th, td { border: 0.5pt solid #b9c0c8; padding: 1.5pt 3.5pt; text-align: left;
         vertical-align: top; }
th { background: #e9edf1; font-weight: bold; }
figure { margin: 5pt 0; text-align: center;
         break-inside: avoid; page-break-inside: avoid; }
figure img { max-width: 100%; height: auto; }
figcaption { font-size: 8pt; color: #333; margin-top: 2pt;
             text-align: justify; }
/* Consecutive figures are paired on one row: three full-width plots cost
   more than a page of the eight the template allows. */
.figrow { display: flex; gap: 10pt; align-items: flex-start;
          break-inside: avoid; page-break-inside: avoid; }
.figrow figure { flex: 1 1 0; margin: 5pt 0; }
.figrow figure img { width: 100%; }
.authors { font-size: 9.5pt; margin: 0 0 6pt 0; text-align: left; }
.authors td, .authors th { border: 0; padding: 0 14pt 1pt 0; }
.authors table { width: auto; font-size: 9.5pt; }
"""


# ---------------------------------------------------------------- inline bits
def inline(text: str) -> str:
    """Inline markdown -> HTML. Code spans are protected before emphasis."""
    stash: list[str] = []

    def keep(m):
        stash.append("<code>" + html.escape(m.group(1)) + "</code>")
        return f"\x00{len(stash) - 1}\x00"

    text = re.sub(r"`([^`]+)`", keep, text)
    text = html.escape(text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"(?<![\w*])\*([^*\n]+?)\*(?![\w*])", r"<em>\1</em>", text)
    text = text.replace("---", "&mdash;")
    return re.sub(r"\x00(\d+)\x00", lambda m: stash[int(m.group(1))], text)


def data_uri(src: str) -> str:
    p = (DOCS / src).resolve()
    if not p.is_file():
        raise SystemExit(f"figure not found: {p}")
    mime = mimetypes.guess_type(p.name)[0] or "image/png"
    return f"data:{mime};base64," + base64.b64encode(p.read_bytes()).decode()


# ------------------------------------------------------------------- blocks
IMG = re.compile(r"^!\[(?P<alt>.*)\]\((?P<src>[^)\s]+)\)(?:\{(?P<attr>[^}]*)\})?\s*$")
HEAD = re.compile(r"^(#{1,6})\s+(.*)$")
OL = re.compile(r"^\d+\.\s+(.*)$")
UL = re.compile(r"^[-*]\s+(.*)$")


def convert(md: str) -> tuple[str, int]:
    md = re.sub(r"\A---\n.*?\n---\n", "", md, flags=re.S)   # YAML front matter
    lines = md.split("\n")
    out: list[str] = []
    figures = 0
    i, n = 0, len(lines)

    while i < n:
        ln = lines[i]

        if ln.startswith("```"):
            i += 1
            buf = []
            while i < n and not lines[i].startswith("```"):
                buf.append(lines[i])
                i += 1
            i += 1
            out.append("<pre>" + html.escape("\n".join(buf)) + "</pre>")
            continue

        m = IMG.match(ln.strip())
        if m:
            group = [m]
            j = i + 1
            while j + 1 < n and not lines[j].strip() and IMG.match(lines[j + 1].strip()):
                group.append(IMG.match(lines[j + 1].strip()))
                j += 2
            figs = []
            for g in group:
                attr = g.group("attr") or ""
                w = re.search(r"width\s*=\s*(\d+)%", attr)
                style = ""
                if w and len(group) == 1:
                    style = f' style="width:{w.group(1)}%"'
                figs.append(
                    "<figure><img src=\"" + data_uri(g.group("src")) + "\"" + style +
                    " alt=\"" + html.escape(g.group("alt")[:120]) + "\">"
                    "<figcaption>" + inline(g.group("alt")) + "</figcaption></figure>")
            out.append("".join(figs) if len(figs) == 1
                       else "<div class=\"figrow\">" + "".join(figs) + "</div>")
            figures += len(figs)
            i = j if len(group) > 1 else i + 1
            continue

        if ln.strip().startswith("|"):
            rows = []
            while i < n and lines[i].strip().startswith("|"):
                rows.append(lines[i].strip())
                i += 1
            out.append(table_html(rows))
            continue

        m = HEAD.match(ln)
        if m:
            lvl = min(len(m.group(1)), 3)
            out.append(f"<h{lvl}>{inline(m.group(2))}</h{lvl}>")
            i += 1
            continue

        if ln.strip() in ("---", "***", "___"):
            out.append("<hr>")
            i += 1
            continue

        if ln.startswith(">"):
            quote = []
            while i < n and lines[i].startswith(">"):
                quote.append(lines[i].lstrip("> ").rstrip())
                i += 1
            out.append("<blockquote>" + inline(" ".join(quote)) + "</blockquote>")
            continue

        if OL.match(ln) or UL.match(ln):
            tag = "ol" if OL.match(ln) else "ol"
            pat = OL if OL.match(ln) else UL
            if UL.match(ln) and not OL.match(ln):
                tag = "ul"
            items = []
            while i < n and pat.match(lines[i]):
                items.append("<li>" + inline(pat.match(lines[i]).group(1)) + "</li>")
                i += 1
            out.append(f"<{tag}>" + "".join(items) + f"</{tag}>")
            continue

        if not ln.strip():
            i += 1
            continue

        para = []
        while i < n and lines[i].strip() and not lines[i].startswith(("#", "|", "```")) \
                and not IMG.match(lines[i].strip()) and lines[i].strip() not in ("---",):
            para.append(lines[i].strip())
            i += 1
        out.append("<p>" + inline(" ".join(para)) + "</p>")

    return "\n".join(out), figures


def table_html(rows: list[str]) -> str:
    def cells(r):
        return [c.strip() for c in r.strip().strip("|").split("|")]

    body = [r for r in rows if not re.fullmatch(r"\|[\s:\-|]+\|", r.strip())]
    if not body:
        return ""
    head, rest = body[0], body[1:]
    # The author block is a borderless two-column list, not a data table.
    klass = ' class="authors"' if cells(head)[:2] == ["Name", "Email"] else ""
    out = [f"<table{klass}><thead><tr>"]
    out += [f"<th>{inline(c)}</th>" for c in cells(head)]
    out.append("</tr></thead><tbody>")
    for r in rest:
        out.append("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in cells(r)) + "</tr>")
    out.append("</tbody></table>")
    return "".join(out)


# --------------------------------------------------------------------- build
def find_browser() -> str:
    for c in EDGE_CANDIDATES:
        if Path(c).is_file():
            return c
    raise SystemExit("no Edge or Chrome found; cannot print to PDF")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--keep-html", action="store_true")
    a = ap.parse_args()

    body, figures = convert(MD.read_text(encoding="utf-8"))
    page = ("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
            "<title>ETL4 Final Project Report</title><style>" + CSS +
            "</style></head><body>" + body + "</body></html>")

    tmp = Path(tempfile.mkdtemp(prefix="etl4report-"))
    html_path = tmp / "report.html"
    html_path.write_text(page, encoding="utf-8")

    out = Path(a.out).resolve()
    if out.exists():
        out.unlink()

    cmd = [
        find_browser(),
        "--headless=new", "--disable-gpu", "--no-first-run",
        "--no-pdf-header-footer", "--print-to-pdf-no-header",
        f"--user-data-dir={tmp / 'profile'}",
        f"--print-to-pdf={out}",
        html_path.as_uri(),
    ]
    subprocess.run(cmd, check=False, capture_output=True, timeout=300)

    if not out.is_file() or out.stat().st_size == 0:
        raise SystemExit("browser produced no PDF")

    pages = "?"
    try:
        from pypdf import PdfReader
        rd = PdfReader(str(out))
        pages = len(rd.pages)
        embedded = sum(len(p.images) for p in rd.pages)
        print(f"embedded images: {embedded} (markdown figures: {figures})")
        if embedded < figures:
            raise SystemExit("figures did not reach the PDF")
    except ImportError:
        print("pypdf not installed: skipping the embedded-image check")

    if a.keep_html:
        kept = out.with_suffix(".html")
        shutil.copy2(html_path, kept)
        print(f"html: {kept}")

    print(f"wrote {out}  ({out.stat().st_size:,} bytes, {pages} pages)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
