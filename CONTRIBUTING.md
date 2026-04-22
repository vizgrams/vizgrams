# Contributing

Thank you for your interest in contributing to vizgrams.

## Getting started

```bash
git clone <repo-url>
cd vizgrams
poetry install --with dev
```

## Running tests

The test suite uses the bundled `example` model and requires no external credentials or `VZ_MODELS_DIR`.

```bash
poetry run pytest tests/ -v
```

To run a specific test file:

```bash
poetry run pytest tests/test_expression_compiler.py -v
```

## Setting up your own models

Create a `.env` file in the project root (gitignored):

```
VZ_MODELS_DIR=/path/to/your/private/models
```

Start the API server:

```bash
./start_api_server.sh
```

## Code style

- Python: standard library style, no enforced formatter at this time.
- TypeScript: Prettier defaults (configured in `ui/`).

## Submitting changes

1. Fork the repository and create a branch from `main`.
2. Make your changes. Add or update tests where relevant.
3. Ensure `python -m pytest tests/` passes.
4. Open a pull request against `main`. All PRs require a review from [@ofenton](https://github.com/ofenton).

## License

By contributing, you agree that your contributions will be licensed under the [Apache License, Version 2.0](LICENSE).
