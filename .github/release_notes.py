"""Print one version's CHANGELOG.md entry as a GitHub release body.

Usage: release_notes.py CHANGELOG.md VERSION. The entry is everything under
"## [VERSION]" up to the next version heading or the link references, without the
heading itself. The changelog is hard-wrapped and a release body renders a line break
as one, so each paragraph and list item is joined onto one line. A version with no
entry, or an empty one, exits nonzero.
"""

import re
import sys


def fenced(lines):
    """Yield (line, inside) with inside True for a fenced code block's lines,
    the fence lines included."""
    fence = None
    for ln in lines:
        m = re.match(r"^\s*(```+|~~~+)", ln)
        if fence is not None:
            yield ln, True
            if m and ln.strip().startswith(fence):
                fence = None
            continue
        if m:
            fence = m.group(1)
            yield ln, True
            continue
        yield ln, False


def entry(text, version):
    head = "## [" + version + "]"
    body = None
    for ln, inside in fenced(text.split("\n")):
        if body is None:
            if not inside and ln.startswith(head):
                body = []
            continue
        if not inside and (ln.startswith("## [") or re.match(r"^\[[^\]]+\]: ", ln)):
            break
        body.append(ln)
    if body is None:
        sys.exit("CHANGELOG.md has no entry for " + version)
    return body


def reflow(lines):
    """Join each paragraph and list item onto one line; keep fenced code blocks,
    tables, block quotes and indented code exactly as written."""
    out = []
    fence = None
    joinable = False  # whether the last output line is prose a continuation may join
    for ln in lines:
        stripped = ln.strip()
        if fence is not None:
            out.append(ln.rstrip())
            if stripped.startswith(fence):
                fence = None
            joinable = False
            continue
        m = re.match(r"^\s*(```+|~~~+)", ln)
        if m:
            fence = m.group(1)
            out.append(ln.rstrip())
            joinable = False
            continue
        verbatim = (stripped.startswith("|") or stripped.startswith(">")
                    or (ln.startswith("    ") and not joinable))
        opens = (stripped == "" or ln.startswith("#") or verbatim
                 or re.match(r"^\s*([-*+]|\d+[.)])\s", ln))
        if joinable and not opens:
            out[-1] = out[-1].rstrip() + " " + stripped
            continue
        out.append(ln.rstrip())
        joinable = stripped != "" and not ln.startswith("#") and not verbatim
    return "\n".join(out).strip()


def main():
    path, version = sys.argv[1], sys.argv[2]
    with open(path, encoding="utf-8") as f:
        text = reflow(entry(f.read(), version))
    if not text:
        sys.exit("the CHANGELOG.md entry for " + version + " is empty")
    print(text)


if __name__ == "__main__":
    main()
