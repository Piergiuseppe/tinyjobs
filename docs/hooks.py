import re
from pathlib import Path

from mkdocs.structure.files import File

FENCE = re.compile(r"^\s*(```|~~~)")
SECTION = re.compile(r"^## (.+)$")
ANCHOR = re.compile(r"\]\(#([^)]+)\)")

pages: dict[str, str] = {}


def slug(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


def split_sections(text: str) -> list[tuple[str, list[str]]]:
    sections: list[tuple[str, list[str]]] = [("", [])]
    fenced = False
    for line in text.splitlines():
        if FENCE.match(line):
            fenced = not fenced
        heading = None if fenced else SECTION.match(line)
        if heading:
            sections.append((heading.group(1).strip(), []))
        else:
            sections[-1][1].append(line)
    return sections


def link_pages(body: str, names: dict[str, str]) -> str:
    return ANCHOR.sub(
        lambda found: f"]({names[found.group(1)]})" if found.group(1) in names else found.group(0),
        body,
    )


def on_config(config):
    readme = Path(config.config_file_path).parent / "README.md"
    (_, preamble), (first, opening), *rest = split_sections(readme.read_text(encoding="utf-8"))
    names = {slug(first): "index.md"} | {slug(title): f"{slug(title)}.md" for title, _ in rest}
    pages.clear()
    pages["index.md"] = link_pages("\n".join([*preamble, f"## {first}", *opening]), names)
    for title, lines in rest:
        pages[names[slug(title)]] = link_pages("\n".join([f"# {title}", *lines]), names)
    titles = {names[slug(title)]: title for title, _ in rest}
    config.nav = [
        {"Home": "index.md"},
        *({titles[name]: name} for name in pages if name != "index.md"),
        {"Design notes": "design.md"},
    ]
    return config


def on_files(files, config):
    for name, content in pages.items():
        files.append(File.generated(config, name, content=content))
    return files


def on_serve(server, config, builder):
    server.watch(str(Path(config.config_file_path).parent / "README.md"))
    return server
