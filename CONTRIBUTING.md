# Contributing to Polar Proxy

Thank you for your interest in contributing to Polar Proxy! This document
outlines the guidelines and requirements for contributing to this project.

Polar Proxy is a database proxy maintained by [Awide Labs](https://awide.tech).
It extends [ProxySQL](https://github.com/sysown/proxysql) with routing and
session consistency for [Awide Polar](https://github.com/awide-labs/polar).

## Before Contributing

- Sign the [Individual Contributor License Agreement](legal/INDIVIDUAL-CLA.md)
  (or have your employer sign the [Corporate CLA](legal/CORPORATE-CLA.md))
  via a [CLA signing issue](https://github.com/awide-labs/polar-proxy/issues/new?template=cla_signing.yml)
  or email to `info@awide.io`. A maintainer adds the `cla-signed` label on your
  pull request after verification.

## Getting Started

1. Fork [`awide-labs/polar-proxy`](https://github.com/awide-labs/polar-proxy)
2. Clone your fork locally
3. Create a new branch for your changes
4. Make your changes following the guidelines below
5. Submit a pull request

## Development Setup

Polar Proxy provides Docker build images for development. Build dependencies
are listed in the Dockerfiles under `docker/images/`.

To build locally:

```bash
git submodule update --init --recursive
make
```

## Making Changes

- Follow the existing code style and patterns in the codebase
- Use clear, descriptive names for variables and functions
- Add comments for complex logic where behavior is non-obvious
- Keep functions focused and maintainable

### C++ guidelines

- Use C++11/14 features appropriately
- Follow RAII principles for resource management
- Consider performance implications — Polar Proxy is performance-critical

## Testing

All pull requests go through automated testing. Run relevant tests locally
before submitting:

- PolarDB integration tests: see [test/polardb/README.md](test/polardb/README.md)
- General test layout: see [test/README.md](test/README.md)

## Commit Messages

Use descriptive messages that explain what changed and why. Reference issue
identifiers in the footer when applicable:

```
<type>: <description>

Refs: GH-1234
```

**Example:** `fix(polardb): correct reader lag timeout handling` with footer
`Refs: GH-456`.

## Pull Request Process

1. Ensure all CI checks pass
2. Ensure the `cla-signed` label is present (see Before Contributing)
3. Update documentation if functionality changes
4. Request review from maintainers
5. Address feedback
6. Once approved, your PR will be merged

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

## License

By contributing to Polar Proxy, you agree that your contributions will be
licensed under the project's GPLv3 license, subject to the signed CLA.

---

Thank you for helping make Polar Proxy better!
