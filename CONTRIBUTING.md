# Contributing to Polar Proxy

Thank you for your interest in contributing to Polar Proxy! This document
outlines the guidelines and requirements for contributing to this project.

Polar Proxy is a database proxy maintained by [Awide Labs](https://awide.tech).
It extends [ProxySQL](https://github.com/sysown/proxysql) with routing and
session consistency for [Awide Polar](https://github.com/awide-labs/polar).
Most of the code is upstream ProxySQL; the changes Polar Proxy makes on top of
it are listed in [`POLAR_CHANGELOG.md`](POLAR_CHANGELOG.md).

## Before Contributing

- Sign the [Individual Contributor License Agreement](legal/INDIVIDUAL-CLA.md)
  (or have your employer sign the [Corporate CLA](legal/CORPORATE-CLA.md))
  via a [CLA signing issue](https://github.com/awide-labs/polar-proxy/issues/new?template=cla_signing.yml)
  or email to `info@awide.io`. A maintainer adds the `cla-signed` label on your
  pull request after verification.

## Getting Started

1. Fork [`awide-labs/polar-proxy`](https://github.com/awide-labs/polar-proxy)
2. Clone your fork locally
3. Create a new branch for your changes from `v3.0`
4. Make your changes following the guidelines below
5. Run tests (see [Testing](#testing))
6. Write commit messages following the
   [Commit Message Format](#commit-message-format)
7. Update `POLAR_CHANGELOG.md` for user-visible changes (see
   [Changelog Requirements](#changelog-requirements))
8. Submit a pull request against `v3.0`

## Development Setup

Polar Proxy provides Docker build images for development. Build dependencies
are listed in the Dockerfiles under `docker/images/`.

To build locally:

```bash
git submodule update --init --recursive
make -j$(nproc)
```

This builds `src/proxysql` with PolarDB support. `make polardb-debug` builds it
from a clean tree with verbose PolarDB traces, and `make core` builds plain
ProxySQL without PolarDB support. The build options are described in
[`doc/polardb-arch/02-BUILD-TOGGLE-AND-LIBPQ.md`](doc/polardb-arch/02-BUILD-TOGGLE-AND-LIBPQ.md).

## Commit Message Format

All commits must follow the
[Conventional Commits](https://www.conventionalcommits.org/) specification.
Reviewers check commit messages before a pull request is merged.

### Format

```
<type>[optional scope]: <description>

[optional body]

Refs: <reference>[, <reference>...]
[optional footer(s)]
```

### Rules

- **Header (first line)**: Must be 72 characters or less
- **Body lines**: Each line must be 72 characters or less (footer lines are
  exempt)
- **Refs footer**: Mandatory - must contain a comma-separated list of issue
  references (e.g., `Refs: GH-1234` or `Refs: GH-1234, GH-5678`). Only one
  `Refs:` footer is allowed per commit

### Valid Types

| Type | Description |
|------|-------------|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation only changes |
| `style` | Changes that do not affect the meaning of the code (formatting, etc.) |
| `refactor` | A code change that neither fixes a bug nor adds a feature |
| `perf` | A code change that improves performance |
| `test` | Adding missing tests or correcting existing tests |
| `build` | Changes that affect the build system or external dependencies |
| `ci` | Changes to CI configuration files and scripts |
| `chore` | Other changes that don't modify src or test files |
| `revert` | Reverts a previous commit |

### Scope Requirement

For `feat`, `fix`, and `perf` commits, we strongly recommend including a scope
to maintain traceable commit history. The scope identifies which component or
feature the commit relates to.

**Why scopes matter:**

- Makes commit history easier to navigate and search
- Enables automated changelog generation per component
- Helps reviewers quickly understand the affected area
- Ensures related changes can be easily tracked together

**Guidelines:**

1. Use the existing scopes where they fit: `polardb` for PolarDB routing and
   consistency, `pgsql` for the PostgreSQL protocol layer, `libpq` for the
   patched libpq, `admin` for the admin interface
2. When introducing a new feature, choose a short, descriptive scope name
3. For subsequent commits related to the same feature, use the same scope
   consistently
4. Check existing scopes before creating a new one to avoid duplicates or typos

To find existing scopes in the repository, run:

```bash
git log --format="%s" | grep -oE '^(feat|fix|perf)\([^)]+\)' | \
  sed 's/[a-z]*(\(.*\))/\1/' | sort -u
```

### Optional Footers

In addition to the mandatory `Refs:` footer, you may include other optional
footers following the Conventional Commits specification. Common examples
include:
- `Skip-changelog: true` - The commit needs no changelog entry although its
  type normally requires one (see
  [Skipping Changelog Updates](#skipping-changelog-updates))
- `Skip-trailing-whitespace: true` - The commit must keep trailing whitespace
  (see [Trailing Whitespace](#trailing-whitespace))
- `See: <URL>` - Reference to external documentation, RFCs, or related
  resources
- `Discussion: <URL>` - Link to a discussion related to this commit

All footers must follow the `token: value` format and are exempt from the
72-character line length limit. They must be in the footer block at the end of
the commit message, e.g. next to `Refs:`; a mention in the body does not
count.

### Exemptions

- **Commits brought by merges**: Commits that are brought in by merge commits
  are exempt, as we have no control over their commit message format. Merge
  commits themselves follow the same rules as any other commit. Upstream
  ProxySQL releases are taken in by rebasing Polar Proxy, so upstream commits
  keep their original messages.

### Examples

```
feat(polardb): route extended Execute reads with LSN waits

Send binary W before Bind/Execute on v15_wait connections so that
extended protocol reads can use readers with the same read-your-writes
guarantee as simple queries.

Refs: GH-1234
```

```
fix(timeout): clear stale connect deadline in RequestEnd

A connect deadline left over from an earlier query made unrelated
queries fail with "Max connect timeout reached ... after 0ms" on
long-lived sessions.

Refs: GH-5678
```

```
docs: update the PolarDB operator guide

Refs: GH-9012
```

## Changelog Requirements

All user-visible changes Polar Proxy makes on top of ProxySQL must be
documented in [`POLAR_CHANGELOG.md`](POLAR_CHANGELOG.md). `CHANGELOG.md` is the
upstream ProxySQL changelog and is not edited in Polar Proxy. We follow the
[Keep a Changelog](https://keepachangelog.com/) specification with one
extension: we include a "Performance" section in addition to the standard
sections.

Each commit that requires a changelog update must modify `POLAR_CHANGELOG.md`
in that same commit. New entries go under `## [Unreleased]`, not under an
existing numbered release. To correct entries post-factum, use a `docs:` or
`docs(scope):` commit; such commits may edit entries in any release.

**Commits that require changelog updates:**
- `feat:` - New features
- `fix:` - Bug fixes
- `perf:` - Performance improvements
- Commits with `BREAKING CHANGE:` in the footer or `!` in the type (e.g.,
  `feat!:` or `feat(api)!:`)

**Commits that do not require changelog updates:**
- `chore:`, `docs:`, `style:`, `refactor:`, `test:`, `build:` and `ci:`

### Skipping Changelog Updates

If a commit with a type that normally requires a changelog update (e.g., `fix:`)
does not actually introduce user-visible changes (for example, it fixes
something introduced in a previous commit within the same pull request, or a
bug in a feature that has not been released yet), add the
`Skip-changelog: true` footer to the commit message. The `Refs:` footer is
still required.

```
fix(polardb): free attached request packet on consistency errors

This fixes a crash introduced in a previous commit within this pull
request. The issue was not present in any released version, so no
user-visible changes are introduced.

Skip-changelog: true
Refs: GH-7890
```

### Sections

- **Added**: New features
- **Changed**: Changes in existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security vulnerability fixes
- **Performance**: Performance improvements (extension to Keep a Changelog)

### User-Focused Language

Changelog entries are written for users of the product, not for developers.
Describe the user-visible impact of the change rather than internals or
implementation details: the symptom, when it happened, and what changes for
the user.

**Do:**

```markdown
- Queries on long-lived client sessions could fail with `Max connect timeout
  reached while reaching hostgroup ... after 0ms` once the session had been
  connected to the backend longer than `pgsql-connect_timeout_server_max`
  (10 seconds by default), although no connection attempt took that long
  (GH-41)
```

**Don't:**

```markdown
- Clear stale max_connect_time in RequestEnd(): find_or_create_backend()
  reuses mybe/server_myds across queries, so the old deadline is carried
  into CONNECTING_SERVER (GH-41)
```

### Entry Format

Each changelog entry (paragraph starting with `- ` at the first column) must
end with an issue reference in the format `(PROJ-NNNN)`, optionally followed
by `.` or `:` (use `:` when sub-items follow). The issue reference must be
preceded by a space, and sub-items must not carry their own references.

```markdown
## [Unreleased]

### Added

- New `pgsql_query_rules.replica_eligible` column (GH-1234)

### Performance

- Reduced lock contention when collecting query digest statistics (GH-2345)

### Removed

- The following deprecated settings have been removed (GH-9012):
  - `pgsql-polardb_old_setting_a`
  - `pgsql-polardb_old_setting_b`
```

### Versions and Releases

Polar Proxy versions have the form `X.Y.Z.N`: `X.Y.Z` is the ProxySQL release
Polar Proxy is based on, and `N` counts Polar Proxy releases on that base,
starting from 0. Releases are tagged `vX.Y.Z.N`. [`POLAR_VERSION`](POLAR_VERSION)
holds the version of the next release.

A release is a `chore(release): stamp <VERSION>` commit that turns
`## [Unreleased]` into `## [<VERSION>] - <YYYY-MM-DD>` and updates the URL
definitions at the end of `POLAR_CHANGELOG.md`, followed by a
`chore: bump version to <NEXT_VERSION>` commit. A rebase on a newer upstream
ProxySQL release sets `POLAR_VERSION` to the new `X.Y.Z.0`.

## Coding Style

All code style guidelines that apply to the ProxySQL codebase also apply to
Polar Proxy. Follow the existing code style and patterns of the surrounding
code, so that upstream changes keep applying cleanly.

### Languages

All code, comments, error and log messages, documentation and commit messages
must be in English.

### C++ Guidelines

- Use C++11/14 features appropriately
- Follow RAII principles for resource management
- Consider performance implications: Polar Proxy is performance-critical, and
  most changes are on the query path
- Use clear, descriptive names for variables and functions
- Add comments for complex logic where behavior is non-obvious
- Keep functions focused and maintainable

### PolarDB-Specific Code

PolarDB support is enabled by the `POLARDB_PROXY` build option (on by
default). Keep PolarDB-specific code under `#if POLARDB_PROXY`, so that
`make core` still builds a plain ProxySQL that behaves like upstream. Prefer
keeping Polar Proxy's code in the `PgSQL_PolarDB*` files over spreading it
through upstream files: every change to an upstream file is a potential
conflict on the next rebase.

Code, comments and tests must not reference issues (`GH-NNNN`); issue
references belong in the `Refs:` footer of the commit message and in
`POLAR_CHANGELOG.md`.

### Trailing Whitespace

Lines must not end in spaces or tabs: in code, documentation (including
`POLAR_CHANGELOG.md`) and tests alike. The one exception is a Markdown hard
line break: in `*.md` files, a line may end in exactly two spaces after text.

Files where trailing whitespace is legitimate are exempt through their
`whitespace` attribute in [`.gitattributes`](.gitattributes), e.g. the
vendored libpq patches under `deps/postgresql/`. These are the same files
`git diff --check` skips, so you can run it before pushing, with `upstream`
being the `awide-labs/polar-proxy` remote:

```bash
git diff --check upstream/v3.0
```

`git rebase --whitespace=fix upstream/v3.0` strips trailing whitespace from
the commits of a branch (except merge commits). A commit that must keep
trailing whitespace, such as a verbatim import of third-party code, can say so
with the `Skip-trailing-whitespace: true` footer.

## Testing

All pull requests go through automated testing. Run relevant tests locally
before submitting:

- PolarDB integration tests need an Awide Polar cluster with a primary and
  two replicas: see [test/polardb/README.md](test/polardb/README.md)
- General test layout: see [test/README.md](test/README.md)

## Pull Request Process

1. Ensure all CI checks pass
2. Ensure the `cla-signed` label is present (see Before Contributing)
3. Update documentation if functionality changes, e.g. the PolarDB
   documentation in [`doc/polardb-arch/`](doc/polardb-arch/README.md)
4. Request review from maintainers
5. Address feedback
6. Once approved, your PR will be merged

During code review, reviewers will check:
- Code design and architecture
- Functionality correctness
- Test coverage
- Code style compliance
- Commit message format
- Documentation completeness
- Performance implications
- Changelog updates for commits with user-visible changes

## Reporting Bugs

Please include in bug reports:

- Polar Proxy / `proxysql` version (`proxysql --version`)
- Steps to reproduce
- Expected vs actual behavior
- Relevant configuration details
- Logs (with sensitive data redacted)

## Feature Requests

For new features:

- Describe the feature or problem clearly
- Explain the use case
- Consider performance implications
- Note any backward compatibility concerns

## Questions and Support

- **GitHub issues:** bugs, features, and questions
- **Email:** `info@awide.io` for contribution or CLA questions
- Search existing issues before creating new ones

## Further Information

The PolarDB support is documented in
[`doc/polardb-arch/`](doc/polardb-arch/README.md): start with the quick start
and the operator guide for its configuration, and the architecture documents
for its internals.

## License

By contributing to Polar Proxy, you agree that your contributions will be
licensed under the project's GPLv3 license, subject to the signed CLA.

---

Thank you for helping make Polar Proxy better!
