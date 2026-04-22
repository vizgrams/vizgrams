# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Inline transform expression parser and evaluator — DEPRECATED.

All functionality has been migrated:
- Expression parsing/evaluation → semantic.expression + engine.python_evaluator
- Filter parsing/compilation    → engine.filter_compiler
- Reference collection          → engine.python_evaluator (collect_refs, collect_enum_refs)

This module is retained as an empty stub to avoid import errors in any code
that has not yet been updated, but all public symbols have been removed.
"""
