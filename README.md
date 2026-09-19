<div align="center">

# Polar Proxy

**A high-performance proxy for MySQL, PostgreSQL, and Awide Polar**

[![official site](https://img.shields.io/badge/official%20site-blueviolet?style=flat)](https://awide.tech/awidepolar)

[![GitHub License](https://img.shields.io/badge/license-GPL--3.0-blue?style=flat)](./LICENSE)
[![github-issues](https://img.shields.io/github/issues/awide-labs/polar-proxy?style=flat&logo=github)](https://github.com/awide-labs/polar-proxy/issues)
[![github-pullrequest](https://img.shields.io/github/issues-pr/awide-labs/polar-proxy?style=flat&logo=github)](https://github.com/awide-labs/polar-proxy/pulls)
[![github-forks](https://img.shields.io/github/forks/awide-labs/polar-proxy?style=flat&logo=github)](https://github.com/awide-labs/polar-proxy/network/members)
[![github-stars](https://img.shields.io/github/stars/awide-labs/polar-proxy?style=flat&logo=github)](https://github.com/awide-labs/polar-proxy/stargazers)

</div>

## Overview

Polar Proxy is a high-performance, protocol-aware database proxy for MySQL and
PostgreSQL. It is maintained by [Awide Labs](https://awide.tech) and extends
[ProxySQL](https://github.com/sysown/proxysql) with routing and session
consistency for [Awide Polar](https://github.com/awide-labs/polar).

The `proxysql` binary name, configuration paths, and admin interfaces remain
compatible with ProxySQL unless documented otherwise.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Quick Start

### Docker

If you have Docker installed, you can pull a Polar Proxy image and run it
directly:

```bash
docker pull awide-labs/polar-proxy:3.0
docker run --name polar-proxy -p 6033:6033 -p 6032:6032 -d awide-labs/polar-proxy:3.0
```

### Build from source

Polar Proxy provides Docker build images for development and packaging. Clone
the repository, initialize submodules, and build:

```bash
git clone https://github.com/awide-labs/polar-proxy.git
cd polar-proxy
git submodule update --init --recursive
make
```

For detailed build and runtime instructions, see [INSTALL.md](INSTALL.md) and
[RUNNING.md](RUNNING.md).

### Awide Polar integration

Polar Proxy adds LSN-based session consistency and reader routing for Awide
Polar PostgreSQL backends. Architecture and operator documentation:

- [PolarDB proxy architecture](doc/polardb-arch/README.md)
- [PolarDB integration tests](test/polardb/README.md)

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Software License

Polar Proxy is released under the
[GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0.html)
(GPLv3). See [LICENSE](./LICENSE) for the full license text.

This project is developed from ProxySQL, which is licensed under GPLv3.
Portions of the codebase retain upstream licensing as described in
[NOTICE](./NOTICE). Polar Proxy also contains third-party components under
other open source licenses; see [NOTICE](./NOTICE) for details.

## Contact

For product information, see the [Awide Polar website](https://awide.tech/awidepolar).

For support or contribution questions, open an issue or email `info@awide.io`.
