import re
from pathlib import Path

from mkdocs.structure.files import File

FENCE = re.compile(r"^\s*(```|~~~)")
SECTION = re.compile(r"^## (.+)$")
ANCHOR = re.compile(r"\]\(#([^)]+)\)")
BADGE = re.compile(r"^\[!\[")
DESIGN_CONTENTS = re.compile(r"^Contents:\n\n(?:- \[[^\]]+\]\(#[^)]+\)\n)+", re.M)

GROUPS = {
    "Get started": [
        "keeping-the-database-from-growing-forever",
        "cron",
        "watching-it",
        "testing-your-tasks",
    ],
    "Integrations": ["fastapi", "django"],
    "Backends": ["postgres", "redis", "other-backends"],
    "About": ["what-you-get", "how-it-compares", "how-it-works", "measured", "status"],
    "Project": ["working-on-it", "tests", "license"],
}
LEFTOVERS = "More"

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


def landing(preamble: list[str]) -> list[str]:
    lines = [line for line in preamble if not BADGE.match(line)]
    title = next(index for index, line in enumerate(lines) if line.startswith("# "))
    lead = next(index for index, line in enumerate(lines) if index > title and line.strip())
    lines[lead] += "\n{ .lead }"
    return lines


def link_pages(body: str, names: dict[str, str]) -> str:
    return ANCHOR.sub(
        lambda found: f"]({names[found.group(1)]})" if found.group(1) in names else found.group(0),
        body,
    )


def grouped_nav(titles: dict[str, str]) -> list[dict]:
    placed = {key for keys in GROUPS.values() for key in keys}
    groups = {
        name: [{titles[key]: f"{key}.md"} for key in keys if key in titles]
        for name, keys in GROUPS.items()
    }
    groups["Get started"].insert(0, {"Introduction": "index.md"})
    groups["Project"].append({"Design notes": "design.md"})
    leftovers = [{title: f"{key}.md"} for key, title in titles.items() if key not in placed]
    if leftovers:
        groups[LEFTOVERS] = leftovers
    return [{name: entries} for name, entries in groups.items() if entries]


def on_config(config):
    readme = Path(config.config_file_path).parent / "README.md"
    (_, preamble), (first, opening), *rest = split_sections(readme.read_text(encoding="utf-8"))
    names = {slug(first): "index.md"} | {slug(title): f"{slug(title)}.md" for title, _ in rest}
    pages.clear()
    pages["index.md"] = link_pages("\n".join([*landing(preamble), f"## {first}", *opening]), names)
    for title, lines in rest:
        pages[names[slug(title)]] = link_pages("\n".join([f"# {title}", *lines]), names)
    config.nav = grouped_nav({slug(title): title for title, _ in rest})
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
