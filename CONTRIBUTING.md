# Contributing to Spatial Ingestion Server

Thank you for your interest in contributing to this project! This repository serves as a high-performance spatial telematics processing portfolio project. Contributions, bug reports, and suggestions are welcome.

## Code of Conduct

Please be respectful and professional in all communications and code reviews. We strive to maintain a clean, collaborative workspace.

## Getting Started

1. **Fork the Repository**: Create a personal copy of the repository on GitHub.
2. **Clone Locally**:
   ```bash
   git clone https://github.com/Ajitesh-stack/Networking.git
   cd Networking
   ```
3. **Set Up Go**: Make sure Go 1.18+ (preferably Go 1.24+) is installed.

## Development Workflow

### 1. Branching
Create a descriptive branch for your changes:
```bash
git checkout -b feature/your-feature-name
# or
git checkout -b bugfix/issue-description
```

### 2. Code Standards
- **Zero External Dependencies**: Keep all logic strictly within the Go standard library. External library imports will be rejected to preserve the core educational/portfolio constraints of the system.
- **Go Style Guide**: Run `go fmt ./...` before submitting your code. Ensure variable naming follows idiomatic Go style (e.g., camelCase, descriptive short names for narrow scopes).
- **Concurrency & Thread Safety**: All cache mutations must use the sharded lock bucketing, and all metric counters must use `sync/atomic`.

### 3. Testing Requirements
Any new features or bug fixes must include unit tests. Ensure all tests pass before proposing changes:
```bash
# Run tests
make test

# Run benchmarks
make bench
```
Your code should maintain or improve the target coverage across packages:
- `/cache` -> 100%
- `/routing` -> 98%+
- `/metrics` -> 100%

### 4. Docker Verification
If making changes that affect deployment:
```bash
# Verify the Docker Compose build compiles and runs
docker-compose up --build
```

## Submitting Pull Requests

1. Commit your changes with clear, descriptive commit messages.
2. Push to your fork and open a Pull Request against the `main` branch.
3. Your PR will trigger the automated GitHub Actions CI pipeline, which runs the linter, compiler, test suite, coverage reporter, and Docker builder. All CI steps must pass for review.
