# Compatibility

tinyjobs is at 0.x: it works and it is measured, and parts of it are still being settled. This page
says what a release may change, how you find out, and what has to be true before a 1.0.

## What is public

Public, and treated with the care described below:

- every name in `tinyjobs.__all__`, whether or not a page here shows it, and the names these
  pages tell you to import from `tinyjobs.testing`, `tinyjobs.dashboard`,
  `tinyjobs.contrib.django`, `tinyjobs.logs` and `tinyjobs.metrics`;
- the options of `@app.task`, `with_options` and `TinyJobs(...)` that these pages show;
- the `tinyjobs` commands and their flags, and their exit statuses where a page states them;
- the Prometheus metric names and labels, and the fields of the JSON log lines;
- the URL schemes, the store options tinyjobs defines itself, and the Django settings keys.

Not public, even where nothing hides it:

- any module, class, method or attribute these pages do not show. Much of the package has no
  leading underscore yet, and `TinyJobs` carries methods the worker and the integrations use
  between themselves; an import that works is not a promise;
- the tables, columns and keys of a store. Read them for a one-off question, as
  [How it works](how-it-works.md) shows, and expect a release to change them through its own
  migration;
- the JSON behind the dashboard, and the exact wording of messages, log lines and command output.

Provisional:

- the [backend protocol](other-backends.md), which is `BaseBackend`, `Backend` and `Capabilities`
  in `tinyjobs.backends`, and its contract tests in `tinyjobs.testing`. Every release that added a
  feature a store must carry has changed them, and some of what the core asks of a store is found
  by name and not yet declared. A third-party store should pin the minor release it was tested on.
  `Job`, `Page`, `Stats`, `QueueStats`, `Execution` and `Finding` are public for the code that
  reads them; the fields a store has to fill in are part of this protocol and may grow with it.

## What a release may change

- From 0.18, a patch release fixes defects and changes nothing else that is public or provisional,
  nor a store's layout, and needs no stop.
- A minor release, 0.x, adds things and may change something public. Every such change is listed
  in the [changelog](https://github.com/Piergiuseppe/tinyjobs/blob/main/CHANGELOG.md) under
  *Changed*, with what to do about it.
- From 0.18, a public name that is renamed or removed keeps working for at least one minor release
  and warns with a `DeprecationWarning` that names its replacement. Where that cannot be done, the
  changelog says so in the release that breaks it.
- `JobStatus` and `ExecutionStatus` may gain members in a minor release, as `held` and `waiting`
  were added; code that handles every status should have a default branch.

## Stores and upgrades

- A release that changes a store's layout upgrades it on the first call. Upgrading to 0.17 itself
  needed every process stopped; every release after it keeps the previous minor release working
  beside it unless its upgrade note says otherwise, so a rolling deploy from one minor to the next
  needs no stop. The upgrade notes are in the changelog, under *Upgrading*.
- A process of a release that a store no longer allows is refused when it starts: every call
  raises and nothing is written. From 0.18 the error is `tinyjobs.NewerStoreError`, and a command
  prints it and exits with status 2; older releases raise a `RuntimeError`. A process already
  running when the store is upgraded does not notice, which is why an upgrade that drops the
  previous minor asks for every process to be stopped first.
- A rolling deploy covers one minor at a time. An upgrade that skips a minor is not one: stop every
  process of the older release first, and follow the upgrade note of each release it passes.
- Going back works only to a release the store still allows, the previous minor unless the upgrade
  note says otherwise. Going further back means restoring a backup taken before the upgrade.

## Python and dependencies

tinyjobs runs on the Python versions its package metadata lists, 3.12 and newer today. From 0.18,
support for a Python version ends in a minor release, after that version has reached its own end
of life.

## Before 1.0

1.0 is a promise that the public surface above, and the backend protocol with it, stops changing
in incompatible ways outside a new major version. It will not be made until:

- the backend protocol declares everything the core asks of a store, and a feature release has
  gone by without changing it;
- what is public is separated in the code from what is not;
- a release that changes a store's layout has passed the upgrade check every release runs, with
  the previous minor working beside it on SQLite, Postgres and Redis. The check exists from 0.18
  and has not yet met a change of layout.
