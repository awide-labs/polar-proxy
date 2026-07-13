#!/usr/bin/env python3
"""
align_ascii_charts.py — check / fix the right-edge alignment of ASCII box charts
embedded in Markdown fenced code blocks.

THE PROBLEM
-----------
Box / flow charts drawn with `+`, `-`, `|` go "ragged" on the right when the
body lines are not padded to a common width, so the closing `|` (and any inner
right borders such as `| |` or `+ |`) drift between columns:

    +---------------------+              +---------------------+
    |  alpha          |                  |  alpha              |
    |  beta longer line     |    -->      |  beta longer line   |
    +----------------+                    +---------------------+
        (ragged)                              (square)

WHAT THIS TOOL DOES  (safe, insert-only, idempotent)
----------------------------------------------------
Inside each ```-fenced block, it finds contiguous "box regions" (runs of framed
lines that include at least one horizontal rule `+---+`) and aligns the
*trailing* border characters of every framed line in that region to a common
set of right-edge columns:

  * body line  -> pads with SPACES before the trailing border(s)
  * rule line  -> extends with DASHES before the final `+`

It aligns each trailing border level independently, so `| ... |` outer edges and
`| ... | |` / `+ |` inner-right edges both line up. Targets are the *widest*
existing position, so the tool only ever INSERTS — it never deletes or truncates
text. If a line's content is already wider than the box edge, it is reported as
an OVERFLOW and left unchanged (reword it by hand).

Scope / non-goals: it squares up the RIGHT side of each box (the common
authoring defect). It does NOT reflow arbitrary interior grid structure, move
arrows, or align left/interior columns. Targets are computed per box region, so
stacked boxes of different widths are not over-padded. Always review with
`--diff` before `--fix`.

USAGE
-----
  align_ascii_charts.py PATH [PATH ...]        # check only; exit 1 if changes needed
  align_ascii_charts.py --diff PATH ...        # check + print a unified diff
  align_ascii_charts.py --fix  PATH ...        # rewrite files in place
  align_ascii_charts.py --fix --diff PATH ...  # rewrite and print what changed

PATH may be a Markdown file or a directory (scanned recursively for *.md).
Exit status: 0 = nothing to do / fixed; 1 = changes needed (check mode) or
overflow lines found; 2 = usage / IO error.
"""

import argparse
import difflib
import sys
from pathlib import Path

BORDER = "|+"
FENCE = "```"


def is_rule(line: str) -> bool:
    """A horizontal box rule: only +, -, |, spaces, with >= 2 corner '+'."""
    s = line.rstrip()
    if not s:
        return False
    if any(c not in "+-| " for c in s.lstrip()):
        return False
    return s.count("+") >= 2 and s.rstrip()[-1] in "+|" and s.lstrip()[0] in "+"


def first_border_col(s: str) -> int:
    for i, ch in enumerate(s):
        if ch in BORDER:
            return i
    return -1


def trailing_run(s: str):
    """Trailing border chars as a RIGHT->LEFT list of (col, char).

    Stops at the first content char and never includes the line's left frame
    edge. Returns None if the last non-space char is not a border char.
    """
    s = s.rstrip()
    if not s or s[-1] not in BORDER:
        return None
    fb = first_border_col(s)
    cols = [len(s) - 1]
    j = len(s) - 2
    while j > fb:
        if s[j] == " ":
            j -= 1
            continue
        if s[j] in BORDER:
            cols.append(j)
            j -= 1
            continue
        break  # content terminates the trailing run
    return [(c, s[c]) for c in cols]  # right -> left


def is_framed(line: str) -> bool:
    return is_rule(line) or trailing_run(line) is not None


def last_border_col(s: str) -> int:
    s = s.rstrip()
    return len(s) - 1 if s and s[-1] in BORDER else -1


def align_region(region):
    """region: list of raw lines forming one box (contiguous framed lines,
    >=1 rule). Squares up the box's OUTER right edge only — the common defect —
    by padding body lines anchored at the box's left edge and extending rules.
    Interior arrows/tracks and stray connector lines are left untouched, so
    nothing is broken; the target is the widest existing edge, so it never
    overflows. Returns (new_lines, overflow_offsets)."""
    if "\t" in "".join(region):
        return [l.rstrip() for l in region], []  # don't column-align tabbed charts

    rules = [l for l in region if is_rule(l)]
    if not rules:
        return [l.rstrip() for l in region], []
    left_edge = min(first_border_col(l) for l in rules)

    def is_body(l):
        # A box body row starts at the box's left border and ends at a border.
        return (not is_rule(l)
                and trailing_run(l) is not None
                and first_border_col(l) == left_edge)

    # TR[r] = widest column of the r-th-from-right trailing border (r=0 outer).
    # Each trailing-border "level" is aligned independently: the outer right
    # edge AND inner edges (e.g. the right edge of a nested box, or a vertical
    # arrow track) all line up. Rules contribute to the outer level only.
    tr = {}
    for l in region:
        if is_rule(l):
            tr[0] = max(tr.get(0, -1), last_border_col(l))
        elif is_body(l):
            for r, (c, _ch) in enumerate(trailing_run(l)):  # right -> left
                tr[r] = max(tr.get(r, -1), c)

    out, overflow = [], []
    for idx, l in enumerate(region):
        s = l.rstrip()
        c = last_border_col(s)
        if is_rule(l):
            out.append(s if c >= tr[0] else s[:c] + "-" * (tr[0] - c) + "+")
            continue
        if not is_body(l):
            out.append(s)  # connector / arrow / label: leave as authored
            continue
        run_lr = list(reversed(trailing_run(s)))  # left -> right
        m = len(run_lr)
        prefix = s[: run_lr[0][0]]  # content left of the leftmost trailing border
        rebuilt, curcol, ok = prefix, len(prefix) - 1, True
        for i, (col, ch) in enumerate(run_lr):
            target = tr[m - 1 - i]                 # this border's from-right level
            left_char = s[col - 1] if col > 0 else " "
            fill = "-" if left_char == "-" else " "  # extend an arrow, else pad
            pad = target - (curcol + 1)
            if pad < 0:                              # content wider than target edge
                ok = False
                break
            rebuilt += fill * pad + ch
            curcol = target
        out.append(rebuilt if ok else s)
        if not ok:
            overflow.append(idx)
    return out, overflow


def process_block(block_lines, base_lineno):
    """Align every box region in one code block. Returns (new_lines, issues)
    where issues is a list of (lineno, kind, text)."""
    new = list(block_lines)
    issues = []
    i = 0
    n = len(block_lines)
    while i < n:
        if not is_framed(block_lines[i]):
            i += 1
            continue
        j = i
        while j < n and is_framed(block_lines[j]):
            j += 1
        region = block_lines[i:j]
        if any(is_rule(x) for x in region) and len(region) >= 2:
            fixed, overflow = align_region(region)
            new[i:j] = fixed
            for off in overflow:
                issues.append((base_lineno + i + off, "overflow",
                               block_lines[i + off].rstrip()))
        i = j
    return new, issues


def process_text(text):
    """Returns (new_text, changed_line_numbers, issues)."""
    lines = text.split("\n")
    out = list(lines)
    in_fence = False
    fence_start = -1
    issues = []
    i = 0
    while i < len(lines):
        stripped = lines[i].lstrip()
        if stripped.startswith(FENCE):
            if not in_fence:
                in_fence = True
                fence_start = i
            else:
                block = lines[fence_start + 1:i]
                fixed, blk_issues = process_block(block, fence_start + 1)
                out[fence_start + 1:i] = fixed
                issues.extend(blk_issues)
                in_fence = False
        i += 1
    new_text = "\n".join(out)
    changed = [n for n, (a, b) in enumerate(zip(lines, out), 1) if a != b]
    return new_text, changed, issues


def iter_md_files(paths):
    for p in paths:
        path = Path(p)
        if path.is_dir():
            yield from sorted(path.rglob("*.md"))
        elif path.is_file():
            yield path
        else:
            print(f"warning: not found: {p}", file=sys.stderr)


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Check/fix right-edge alignment of ASCII box charts in Markdown.")
    ap.add_argument("paths", nargs="+", help="Markdown files or directories")
    ap.add_argument("--fix", action="store_true", help="rewrite files in place")
    ap.add_argument("--diff", action="store_true", help="print a unified diff")
    args = ap.parse_args(argv)

    any_changes = False
    any_overflow = False
    files = list(iter_md_files(args.paths))
    if not files:
        print("no Markdown files found", file=sys.stderr)
        return 2

    for path in files:
        try:
            original = path.read_text(encoding="utf-8")
        except OSError as e:
            print(f"error reading {path}: {e}", file=sys.stderr)
            return 2
        new_text, changed, issues = process_text(original)

        for lineno, kind, txt in issues:
            if kind == "overflow":
                any_overflow = True
                print(f"{path}:{lineno}: OVERFLOW (content wider than box edge; "
                      f"left unchanged): {txt}")

        if new_text != original:
            any_changes = True
            if args.diff:
                diff = difflib.unified_diff(
                    original.splitlines(True), new_text.splitlines(True),
                    fromfile=f"{path} (before)", tofile=f"{path} (after)")
                sys.stdout.writelines(diff)
            if args.fix:
                path.write_text(new_text, encoding="utf-8")
                print(f"fixed {path}: {len(changed)} line(s) realigned")
            else:
                print(f"{path}: {len(changed)} line(s) need realignment "
                      f"(run with --fix)")
        # else: silent when clean

    if args.fix:
        return 0
    return 1 if (any_changes or any_overflow) else 0


if __name__ == "__main__":
    sys.exit(main())
