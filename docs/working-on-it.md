# Working on it

Nothing to install beyond an interpreter, but `python3` on macOS is still 3.9, which is too old. A
venv saves you from remembering:

```bash
uv venv --python 3.12 .venv     # or: python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Then `python`, `tinyjobs` and the example all point at the right interpreter:

```bash
python -m examples.demo
tinyjobs worker --app examples.demo:app --concurrency 4
```

Importing the package on anything older than 3.12 raises with the version it found and the path to it.
