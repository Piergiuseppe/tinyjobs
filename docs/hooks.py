import re
from pathlib import Path

from mkdocs.structure.files import File

BADGE = re.compile(r"^\[!\[")
DOCS_LINK = re.compile(r"\]\(https://tinyjobs\.readthedocs\.io/en/latest/([a-z0-9-]+)/(#[^)]*)?\)")
ASSETS = "docs/assets/"
DESIGN_CONTENTS = re.compile(r"^Contents:\n\n(?:- \[[^\]]+\]\(#[^)]+\)\n)+", re.M)

pages: dict[str, str] = {}


def landing(text: str) -> str:
    lines = [line for line in text.splitlines() if not BADGE.match(line)]
    title = next(index for index, line in enumerate(lines) if line.startswith("# "))
    lead = next(index for index, line in enumerate(lines) if index > title and line.strip())
    lines[lead] += "\n{ .lead }"
    body = "\n".join(lines).replace(ASSETS, "assets/")
    return DOCS_LINK.sub(lambda found: f"]({found.group(1)}.md{found.group(2) or ''})", body)


def on_config(config):
    readme = Path(config.config_file_path).parent / "README.md"
    pages.clear()
    pages["index.md"] = landing(readme.read_text(encoding="utf-8"))
    return config


def on_files(files, config):
    for name, content in pages.items():
        files.append(File.generated(config, name, content=content))
    return files


def on_page_markdown(markdown, page, config, files):
    if page.file.src_uri == "design.md":
        return DESIGN_CONTENTS.sub("", markdown, count=1)
    return markdown


def on_serve(server, config, builder):
    server.watch(str(Path(config.config_file_path).parent / "README.md"))
    return server
