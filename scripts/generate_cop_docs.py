#!/usr/bin/env python3
"""Generate docs/index.html from cop-results.md."""

from __future__ import annotations

import html
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "cop-results.md"
DESTINATION = ROOT / "docs" / "index.html"
RAW_LOG_BASE = "https://raw.githubusercontent.com/TakehideSoh/improving-kat/main/"

VISIBLE_RESULT_TOKENS = (
    "95bf8a8a-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd-20260605-95bf8a8a-q10609",
    "a3aa7f34-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd-20260604-a3aa7f34-q10609",
    "bac40e3b-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd-20260603-bac40e3b-q10609",
    "2c5208ae-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd-20260603-2c5208ae-q10609",
    "ee83b5a8-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd",
    "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd-20260602-ee83b5a8-q10609",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd",
    "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-20260531-630ede96-q10609",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase",
    "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase-20260531-630ede96-q10609",
    "2ccb88e9-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd",
    "kat-cop1000-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd-20260526-2ccb88e9-q10609",
    "0d68ca9c-dirty-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd",
    "kat-cop1000-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd-20260529-0d68ca9c-dirty-q10609",
    "ace64g-rr-20260505",
    "pycsp3-extra-ortools-20260505",
    "pycsp3-extra-ortools-1t-verbose1-20260509",
)


CSS = """:root { color-scheme: light; }
body { margin: 0; padding: 2rem; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.55; color: #1f2937; background: #f8fafc; }
main { max-width: 1440px; margin: 0 auto; background: white; padding: 2rem; border: 1px solid #e5e7eb; border-radius: 12px; box-shadow: 0 1px 3px rgb(15 23 42 / 0.08); }
h1, h2 { border-bottom: 1px solid #e5e7eb; padding-bottom: .25rem; }
a { color: #2563eb; text-decoration: none; }
a:hover { text-decoration: underline; }
code { background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 4px; padding: 0 .25em; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .92em; }
.table-wrap { overflow: auto; max-height: 82vh; margin: 1rem 0 2rem; border: 1px solid #d1d5db; }
table { border-collapse: separate; border-spacing: 0; min-width: max-content; font-size: .9rem; }
th, td { border-right: 1px solid #d1d5db; border-bottom: 1px solid #d1d5db; padding: .35rem .5rem; vertical-align: top; background: white; }
th { background: #f1f5f9; }
tr:nth-child(even) td { background: #fafafa; }
.sticky-col-label { position: sticky; top: 0; z-index: 5; box-shadow: 0 1px 0 #d1d5db; }
.sticky-row-label { position: sticky; z-index: 4; box-shadow: 1px 0 0 #d1d5db; }
.sticky-row-label-1 { left: 0; }
.sticky-corner-label { z-index: 7; }
.column-controls { margin: 1rem 0; padding: .75rem; border: 1px solid #d1d5db; border-radius: 8px; background: #f8fafc; }
.column-controls-title { margin: 0 0 .5rem; font-weight: 600; }
.column-control-actions { display: flex; flex-wrap: wrap; gap: .5rem; margin-bottom: .75rem; }
.column-control-actions button { border: 1px solid #cbd5e1; border-radius: 6px; background: white; padding: .25rem .6rem; cursor: pointer; }
.column-control-actions button:hover { background: #f1f5f9; }
.column-control-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(22rem, 1fr)); gap: .35rem .75rem; }
.column-control-list label { display: flex; gap: .35rem; align-items: flex-start; }
"""


SCRIPT = """(() => {
  const DEFAULT_COP1000_COLUMNS = new Set([
    '95bfAU',
    'a3aaAU',
    'bacAU',
    '2c52AU',
    'ee83PS',
    '630PS',
    '630NP',
    '2ccOrd',
    '0d68Lg',
    'ACE64G',
    'ORT28',
    'ORT1V',
  ]);
  const COP1000_COLUMN_FULL_NAMES = {
    '95bfAU': '95bf8a8a portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objauto cumexact time-rd',
    'a3aaAU': 'a3aa7f34 portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objauto cumexact time-rd',
    'bacAU': 'bac40e3b portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objauto cumexact time-rd',
    '2c52AU': '2c5208ae portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objauto cumexact time-rd',
    'ee83PS': 'ee83b5a8 portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objguided phase-save cumexact time-rd',
    '630PS': '630ede96 portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge cumexact time-rd',
    '630NP': '630ede96 portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge cumexact time-rd no-phase',
    '2ccOrd': '2ccb88e9 order-ge guarded-basic directexpr inchard cnf mdd-tl link-cost eqne2 cumexact time-rd',
    '0d68Lg': '0d68ca9c-dirty log-scop guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg cumexact time-rd',
    'ACE64G': 'ACE 64G rr 20260505',
    'ORT28': 'pycsp3-extra OR-Tools 28t 20260505',
    'ORT1V': 'pycsp3-extra OR-Tools 1t verbose1 20260509',
  };

  function setupStickyLabels() {
    document.querySelectorAll('.table-wrap table').forEach((table) => {
      const headerCells = table.tHead ? Array.from(table.tHead.rows[0].cells) : [];
      headerCells.forEach((cell) => cell.classList.add('sticky-col-label'));
      const firstColumnWidth = Math.ceil(headerCells[0]?.getBoundingClientRect().width || 0);
      Array.from(table.rows).forEach((row) => {
        const first = row.cells[0];
        const second = row.cells[1];
        if (first) {
          first.classList.add('sticky-row-label', 'sticky-row-label-1');
          first.style.left = '0px';
        }
        if (second) {
          second.classList.add('sticky-row-label', 'sticky-row-label-2');
          second.style.left = `${firstColumnWidth}px`;
        }
      });
      if (headerCells[0]) headerCells[0].classList.add('sticky-corner-label');
      if (headerCells[1]) headerCells[1].classList.add('sticky-corner-label');
    });
  }

  function cop1000Table() {
    const heading = document.getElementById('cop1000-instance-table');
    let node = heading ? heading.nextElementSibling : null;
    while (node) {
      if (node.classList?.contains('table-wrap')) {
        const table = node.querySelector('table');
        if (table) return table;
      }
      node = node.nextElementSibling;
    }
    return null;
  }

  function setColumnVisible(table, index, visible) {
    Array.from(table.rows).forEach((row) => {
      const cell = row.cells[index];
      if (cell) cell.hidden = !visible;
    });
  }

  function applyColumnSelection(table, checkboxes) {
    checkboxes.forEach((checkbox) => {
      setColumnVisible(table, Number(checkbox.value), checkbox.checked);
    });
    setupStickyLabels();
  }

  function setDefaultSelection(checkboxes) {
    checkboxes.forEach((checkbox) => {
      checkbox.checked = DEFAULT_COP1000_COLUMNS.has(checkbox.dataset.columnName || '');
    });
  }

  function setupCop1000ColumnControls() {
    const table = cop1000Table();
    if (!table || !table.tHead || document.getElementById('cop1000-column-controls')) return;

    const heading = document.getElementById('cop1000-instance-table');
    const controls = document.createElement('section');
    controls.id = 'cop1000-column-controls';
    controls.className = 'column-controls';
    controls.innerHTML = `
      <p class="column-controls-title">COP1000 instance table columns</p>
      <div class="column-control-actions">
        <button type="button" data-action="default">Default</button>
        <button type="button" data-action="all">Show all</button>
        <button type="button" data-action="none">Hide solver columns</button>
      </div>
      <div class="column-control-list"></div>
    `;
    heading.insertAdjacentElement('afterend', controls);

    const list = controls.querySelector('.column-control-list');
    const headerCells = Array.from(table.tHead.rows[0].cells);
    const checkboxes = headerCells.slice(2).map((cell, offset) => {
      const index = offset + 2;
      const label = document.createElement('label');
      const checkbox = document.createElement('input');
      const shortName = cell.textContent.trim();
      const fullName = COP1000_COLUMN_FULL_NAMES[shortName] || shortName;
      checkbox.type = 'checkbox';
      checkbox.value = String(index);
      checkbox.dataset.columnName = shortName;
      checkbox.checked = DEFAULT_COP1000_COLUMNS.has(checkbox.dataset.columnName);
      cell.title = fullName;
      label.append(checkbox, document.createTextNode(fullName));
      list.append(label);
      checkbox.addEventListener('change', () => applyColumnSelection(table, checkboxes));
      return checkbox;
    });

    controls.addEventListener('click', (event) => {
      const button = event.target.closest('button[data-action]');
      if (!button) return;
      if (button.dataset.action === 'default') setDefaultSelection(checkboxes);
      if (button.dataset.action === 'all') checkboxes.forEach((checkbox) => { checkbox.checked = true; });
      if (button.dataset.action === 'none') checkboxes.forEach((checkbox) => { checkbox.checked = false; });
      applyColumnSelection(table, checkboxes);
    });

    applyColumnSelection(table, checkboxes);
  }

  function initialize() {
    setupCop1000ColumnControls();
    setupStickyLabels();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initialize);
  } else {
    initialize();
  }
  window.addEventListener('resize', setupStickyLabels);
})();"""


def slugify(text: str) -> str:
    slug = re.sub(r"[^\w\- ]+", "", text.lower()).strip()
    return re.sub(r"\s+", "-", slug)


def split_row(line: str) -> list[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for char in line:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "|":
            cells.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    cells.append("".join(current).strip())
    return cells


def is_alignment_row(line: str) -> bool:
    cells = split_row(line)
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell.strip()) for cell in cells)


def inline_markdown(text: str) -> str:
    def html_link(match: re.Match[str]) -> str:
        label = match.group(1)
        url = match.group(2)
        if url.startswith("docs/logs/"):
            url = RAW_LOG_BASE + url
        elif url.startswith("logs/"):
            url = RAW_LOG_BASE + "docs/" + url
        elif url.startswith("docs/"):
            url = url.removeprefix("docs/")
        elif not re.match(r"^(?:[a-z][a-z0-9+.-]*:|#|\.\./)", url, re.IGNORECASE):
            url = "../" + url
        return f'<a href="{html.escape(url, quote=True)}">{label}</a>'

    escaped = html.escape(text)
    escaped = re.sub(
        r"`([^`]+)`",
        lambda match: f"<code>{match.group(1)}</code>",
        escaped,
    )
    escaped = re.sub(
        r"\[([^\]]+)\]\(([^)]+)\)",
        html_link,
        escaped,
    )
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    return escaped


def table_html(lines: list[str]) -> str:
    header = split_row(lines[0])
    alignment = split_row(lines[1])
    rows = [split_row(line) for line in lines[2:]]
    if header and header[0] in {"run", "対象"}:
        rows = [
            row
            for row in rows
            if row and any(token in row[0] for token in VISIBLE_RESULT_TOKENS)
        ]
    align_right = [cell.endswith(":") and not cell.startswith(":") for cell in alignment]

    def cell_attrs(index: int) -> str:
        return ' style="text-align: right;"' if index < len(align_right) and align_right[index] else ""

    output = ["<div class=\"table-wrap\"><table>"]
    output.append(
        "<thead><tr>"
        + "".join(f"<th{cell_attrs(i)}>{inline_markdown(cell)}</th>" for i, cell in enumerate(header))
        + "</tr></thead>"
    )
    output.append("<tbody>")
    for row in rows:
        output.append(
            "<tr>"
            + "".join(f"<td{cell_attrs(i)}>{inline_markdown(cell)}</td>" for i, cell in enumerate(row))
            + "</tr>"
        )
    output.append("</tbody>")
    output.append("</table></div>")
    return "\n".join(output)


def markdown_to_html(markdown: str) -> str:
    lines = markdown.splitlines()
    output: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if line.startswith("<!--"):
            output.append(line)
            i += 1
            continue
        if line.startswith("```"):
            i += 1
            code_lines: list[str] = []
            while i < len(lines) and not lines[i].startswith("```"):
                code_lines.append(lines[i])
                i += 1
            if i < len(lines):
                i += 1
            output.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
            continue
        if line.startswith("#"):
            level = len(line) - len(line.lstrip("#"))
            text = line[level:].strip()
            output.append(f'<h{level} id="{slugify(text)}">{inline_markdown(text)}</h{level}>')
            i += 1
            continue
        if line.startswith("|") and i + 1 < len(lines) and is_alignment_row(lines[i + 1]):
            table_lines = [line, lines[i + 1]]
            i += 2
            while i < len(lines) and lines[i].startswith("|"):
                table_lines.append(lines[i])
                i += 1
            output.append(table_html(table_lines))
            continue
        if line.startswith("- "):
            items: list[str] = []
            while i < len(lines) and lines[i].startswith("- "):
                item_parts = [lines[i][2:].strip()]
                i += 1
                while i < len(lines) and lines[i].startswith("  ") and lines[i].strip():
                    item_parts.append(lines[i].strip())
                    i += 1
                items.append(f"<li>{inline_markdown(' '.join(item_parts))}</li>")
            output.append("<ul>\n" + "\n".join(items) + "\n</ul>")
            continue
        paragraph = [line.strip()]
        i += 1
        while (
            i < len(lines)
            and lines[i].strip()
            and not lines[i].startswith(("#", "<!--", "|", "- "))
        ):
            paragraph.append(lines[i].strip())
            i += 1
        output.append(f"<p>{inline_markdown(' '.join(paragraph))}</p>")
    return "\n".join(output)


def main() -> None:
    body = markdown_to_html(SOURCE.read_text())
    html_text = f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>COP Results</title>
<style>
{CSS}
</style>
</head>
<body>
<main>
{body}
</main>
<script>
{SCRIPT}
</script>
</body>
</html>
"""
    DESTINATION.parent.mkdir(parents=True, exist_ok=True)
    DESTINATION.write_text(html_text)


if __name__ == "__main__":
    main()
