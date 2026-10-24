"""
Builds one documentation module: Markdown source -> HTML -> PDF.

    python documentation/technical_modules/_build/build_module.py <source.md> <output.pdf> <mermaid.min.js>

- `{{code <path> <first>-<last>}}` is replaced by those lines of the real
  source file (relative to the project root), with their line numbers, so
  code is never retyped or shortened. Quoted secrets are redacted.
- `{{coverage <path> <first>-<last> ...}}` is not printed; it declares which
  lines of a file the module covers, for check_coverage().
- ```mermaid blocks are drawn by Mermaid in the page; $...$ and $$...$$ are
  left for KaTeX when a module has equations (--katex <dir>).
- Headless Chrome prints the page once Mermaid has finished (title READY).
"""
import argparse
import html
import os
import re
import subprocess
from pathlib import Path

import markdown

ROOT = Path(__file__).resolve().parents[3]
# "@train/<file>" = the separate training project (TRAINING_PROJECT_DIR).
TRAINING = Path(os.getenv("TRAINING_PROJECT_DIR", r"C:\Users\user\OneDrive\Pictures\crop_disease_ews"))


def source_path(path: str) -> Path:
    return TRAINING / path[len("@train/"):] if path.startswith("@train/") else ROOT / path


def shown_path(path: str) -> str:
    return "crop_disease_ews/" + path[len("@train/"):] if path.startswith("@train/") else path
CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
CODE_RE = re.compile(r"^\{\{code (\S+) (\d+)-(\d+)\}\}$", re.M)
SECRET_RE = re.compile(r"""((?:password|secret|api_key|token)\w*\s*=\s*)(["'])[^"']+\2""", re.I)
YAML_SECRET_RE = re.compile(r"^(\s*\w*(?:PASSWORD|SECRET|TOKEN)\w*:\s*)(\S.*)$", re.I)
URL_SECRET_RE = re.compile(r"(://[^:/\s@]+:)([^@\s]+)(@)")


def redact(line: str) -> str:
    line = SECRET_RE.sub(r"\1\2***\2", line)
    line = YAML_SECRET_RE.sub(r"\1***", line)
    return URL_SECRET_RE.sub(r"\1***\3", line)
LANG = {".py": "python", ".yml": "yaml", ".yaml": "yaml", ".sql": "sql", ".kt": "kotlin", ".txt": "text"}


def code_block(path: str, first: int, last: int) -> str:
    lines = source_path(path).read_text(encoding="utf-8").split("\n")
    if not 1 <= first <= last <= len(lines):
        raise SystemExit(f"{path} {first}-{last}: the file has {len(lines)} lines")
    width = len(str(last))
    body = "\n".join(f"{n:>{width}} │ {redact(lines[n - 1])}"
                     for n in range(first, last + 1))
    return f"*`{shown_path(path)}`, lines {first}–{last}*\n\n```{LANG.get(Path(path).suffix, 'text')}\n{body}\n```"


def covered_lines(text: str) -> dict:
    """{path: set of line numbers} from the {{code}} markers."""
    out = {}
    for path, a, b in CODE_RE.findall(text):
        out.setdefault(path, set()).update(range(int(a), int(b) + 1))
    return out


def check_coverage(text: str) -> list:
    """Lines of each declared file that no {{code}} block shows (blank lines ignored)."""
    gaps = []
    for path, spec in re.findall(r"^\{\{coverage (\S+) (.+)\}\}$", text, re.M):
        want = set()
        for part in spec.split():
            a, b = part.split("-")
            want.update(range(int(a), int(b) + 1))
        lines = source_path(path).read_text(encoding="utf-8").split("\n")
        shown = covered_lines(text).get(path, set())
        missing = sorted(n for n in want - shown if n <= len(lines) and lines[n - 1].strip())
        if missing:
            gaps.append(f"{path}: {len(missing)} line(s) not shown, e.g. {missing[:10]}")
    return gaps


def to_html(text: str, mermaid_js: str, katex_dir: Path = None) -> str:
    text = text.replace("\r\n", "\n")
    text = re.sub(r"^\{\{coverage .+\}\}\n?", "", text, flags=re.M)
    text = CODE_RE.sub(lambda m: code_block(m.group(1), int(m.group(2)), int(m.group(3))), text)

    blocks = []
    def keep(m):
        blocks.append(m.group(1))
        return f"\n\nMERMAIDBLOCK{len(blocks) - 1}\n\n"
    text = re.sub(r"```mermaid\n(.*?)\n```", keep, text, flags=re.S)

    math = []
    if katex_dir:
        def keep_math(m):
            math.append(m.group(0))
            return f"MATHBLOCK{len(math) - 1}X"
        text = re.sub(r"\$\$.+?\$\$|\$[^$\n]+?\$", keep_math, text, flags=re.S)

    # Python-Markdown wants 4-space list nesting and a blank line before a list.
    out, fence = [], False
    for line in text.split("\n"):
        if line.startswith("```"):
            fence = not fence
        if not fence:
            m = re.match(r"^( +)([-*]|\d+\.) ", line)
            if m:
                line = " " * (len(m.group(1)) * 2) + line[len(m.group(1)):]
            if re.match(r"^\s*([-*]|\d+\.) ", line) and out and out[-1].strip() and \
                    not re.match(r"^\s*([-*]|\d+\.) ", out[-1]) and not out[-1].lstrip().startswith(("|", ">")):
                out.append("")
        out.append(line)
    md = markdown.Markdown(extensions=["tables", "fenced_code", "toc", "sane_lists"],
                           extension_configs={"toc": {"toc_depth": "1-3"}})
    body = md.convert("\n".join(out))
    for i, src in enumerate(blocks):
        body = body.replace(f"<p>MERMAIDBLOCK{i}</p>", f'<pre class="mermaid">{html.escape(src)}</pre>')
    for i, src in enumerate(math):
        body = body.replace(f"MATHBLOCK{i}X", html.escape(src))
    toc = md.toc.replace('<div class="toc">', "").rsplit("</div>", 1)[0]

    katex_head = katex_run = ""
    if katex_dir:
        katex_css = (katex_dir / "katex.min.css").read_text(encoding="utf-8").replace(
            "url(fonts/", f"url({(katex_dir / 'fonts').resolve().as_uri()}/")
        katex_head = (f"<style>{katex_css}</style>"
                      f"<script>{(katex_dir / 'katex.min.js').read_text(encoding='utf-8')}</script>"
                      f"<script>{(katex_dir / 'contrib' / 'auto-render.min.js').read_text(encoding='utf-8')}</script>")
        katex_run = ("renderMathInElement(document.body, {delimiters: [{left: '$$', right: '$$', display: true},"
                     "{left: '$', right: '$', display: false}], throwOnError: false});")
    css = (Path(__file__).with_name("style.css")).read_text(encoding="utf-8")
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>LOADING</title>
<style>{css}</style>{katex_head}<script>{mermaid_js}</script></head><body>
<div class="toc-page"><h1>Konteúdu</h1><div class="toc">{toc}</div></div>
{body}
<script>
{katex_run}
mermaid.initialize({{ startOnLoad: false, theme: 'neutral', securityLevel: 'loose' }});
mermaid.run({{ querySelector: 'pre.mermaid' }})
  .then(() => {{ document.title = 'READY'; }})
  .catch(e => {{ document.title = 'MERMAID-ERROR ' + e; }});
</script></body></html>"""


def main():
    p = argparse.ArgumentParser()
    p.add_argument("source"); p.add_argument("pdf"); p.add_argument("mermaid_js")
    p.add_argument("--katex", help="folder with katex.min.js/.css and contrib/auto-render.min.js")
    args = p.parse_args()
    text = Path(args.source).read_text(encoding="utf-8")
    gaps = check_coverage(text)
    for g in gaps:
        print("COVERAGE GAP:", g)
    page = to_html(text, Path(args.mermaid_js).read_text(encoding="utf-8"),
                   Path(args.katex) if args.katex else None)
    pdf = Path(args.pdf).resolve()
    html_path = pdf.with_suffix(".html")
    html_path.write_text(page, encoding="utf-8")
    subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    "--virtual-time-budget=60000", f"--print-to-pdf={pdf}", html_path.as_uri()],
                   check=True, capture_output=True)
    html_path.unlink()
    print("wrote", pdf, "(coverage gaps: %d)" % len(gaps))


if __name__ == "__main__":
    main()
