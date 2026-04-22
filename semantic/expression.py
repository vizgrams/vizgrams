# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Expression AST, parser, and dataclass for the semantic expression engine (ADR-116 Phase 3)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

# ---------------------------------------------------------------------------
# AST types
# ---------------------------------------------------------------------------

class AggFunc(StrEnum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"


@dataclass
class Lit:
    """A literal value (number, string, bool, or None/NULL)."""
    value: str | int | float | bool | None


@dataclass
class FieldRef:
    """A dotted field reference, e.g. ['PullRequest', 'cycle_time'] or ['cycle_time']."""
    parts: list[str]


@dataclass
class BinOp:
    """Binary operator expression — arithmetic or comparison."""
    left: Expr
    op: str   # '+' | '-' | '*' | '/' | '=' | '!=' | '<' | '<=' | '>' | '>='
    right: Expr


@dataclass
class AggExpr:
    """Aggregate function expression."""
    func: AggFunc
    expr: Expr


@dataclass
class CaseWhenExpr:
    """CASE WHEN condition THEN then_expr [ELSE else_expr] END."""
    when: Expr
    then: Expr
    else_: Expr | None = None


@dataclass
class FuncCallExpr:
    """Scalar SQL function call, optionally with keyword arguments.

    e.g. datetime_diff(created_at, merged_at, unit="hours")
         → args=[FieldRef(['created_at']), FieldRef(['merged_at'])]
         → kwargs={"unit": Lit("hours")}
    """
    name: str
    args: list[Expr]
    kwargs: dict[str, Expr] = field(default_factory=dict)


@dataclass
class UnaryExpr:
    """Unary operator: '-', 'not', 'is_null', 'is_not_null'."""
    op: str
    expr: Expr


@dataclass
class WindowExpr:
    """Window function: func(arg).over(partition_by..., order_by).

    Convention for .over() positional args:
      - All args except the last are partition_by columns.
      - The last arg is order_by.

    e.g. lag(released_at).over(product_key, released_at)
         → func="lag", arg=FieldRef(['released_at'])
         → partition_by=[FieldRef(['product_key'])]
         → order_by=[FieldRef(['released_at'])]
    """
    func: str
    arg: Expr | None
    partition_by: list[Expr]
    order_by: list[Expr]


@dataclass
class ListLit:
    """A list literal, e.g. ["open", "active"]. Used with in / not_in."""
    values: list  # list[Expr]


@dataclass
class InExpr:
    """Field in [...] / field not_in [...] filter expression."""
    expr: object  # Expr
    values: list  # list[Expr] — always a ListLit's values
    negated: bool


@dataclass
class DurationLit:
    """A duration literal, e.g. 7d, 4w. Used with now() - Nd."""
    amount: int
    unit: str  # 'd' or 'w'


@dataclass
class MethodCallExpr:
    """Method call on an expression, e.g. field.startswith("x"), field.is_null()."""
    expr: object  # Expr — the receiver
    method: str
    args: list  # list[Expr]


Expr = (
    Lit | FieldRef | BinOp | AggExpr | CaseWhenExpr | FuncCallExpr | UnaryExpr | WindowExpr
    | ListLit | InExpr | DurationLit | MethodCallExpr
)


# ---------------------------------------------------------------------------
# ExpressionFeatureDef
# ---------------------------------------------------------------------------

@dataclass
class ExpressionFeatureDef:
    feature_id: str
    name: str
    entity_type: str
    entity_key: str
    data_type: str
    materialization_mode: str
    expression: Expr
    description: str | None = None
    dependencies: list[str] = field(default_factory=list)
    ttl_seconds: int | None = None
    version: int = 1
    feature_type: str = "expression"


# ---------------------------------------------------------------------------
# Tokeniser
# ---------------------------------------------------------------------------

_EXPR_TOKEN_RE = re.compile(
    r"""
    \s*(?:
        (?P<STRING>"[^"]*"|'[^']*')        |  # string literal
        (?P<DURATION>\d+[dw])               |  # duration literal (must come before NUMBER)
        (?P<NUMBER>\d+(?:\.\d+)?)           |  # number literal
        (?P<PLUS>\+)                        |  # +
        (?P<STAR>\*)                        |  # *
        (?P<SLASH>/)                        |  # /
        (?P<MINUS>-)                        |  # -
        (?P<NEQ>!=|<>)                      |  # != <>
        (?P<LTE><=)                         |  # <=
        (?P<GTE>>=)                         |  # >=
        (?P<LT><)                           |  # <
        (?P<GT>>)                           |  # >
        (?P<EQ>=)                           |  # =
        (?P<IDENT>[A-Za-z_]\w*)             |  # identifier
        (?P<DOT>\.)                         |  # dot
        (?P<LPAREN>\()                      |  # (
        (?P<RPAREN>\))                      |  # )
        (?P<LBRACKET>\[)                    |  # [
        (?P<RBRACKET>\])                    |  # ]
        (?P<COMMA>,)                           # ,
    )\s*
    """,
    re.VERBOSE,
)

_EXPR_TOKEN_KINDS = (
    "STRING", "DURATION", "NUMBER", "PLUS", "STAR", "SLASH", "MINUS",
    "NEQ", "LTE", "GTE", "LT", "GT", "EQ",
    "IDENT", "DOT", "LPAREN", "RPAREN", "LBRACKET", "RBRACKET", "COMMA",
)

_COMPARISON_OPS = {"EQ": "=", "NEQ": "!=", "LT": "<", "LTE": "<=", "GT": ">", "GTE": ">="}

_AGG_FUNC_NAMES = {f.value for f in AggFunc}


def _tokenize_expr(text: str) -> list[tuple[str, str]]:
    tokens = []
    pos = 0
    for m in _EXPR_TOKEN_RE.finditer(text):
        if m.start() != pos:
            bad = text[pos:m.start()].strip()
            if bad:
                raise ValueError(
                    f"Unexpected character(s) {bad!r} in expression at position {pos}"
                )
        for kind in _EXPR_TOKEN_KINDS:
            val = m.group(kind)
            if val is not None:
                tokens.append((kind, val))
                break
        pos = m.end()
    trailing = text[pos:].strip()
    if trailing:
        raise ValueError(f"Unexpected trailing characters {trailing!r} in expression")
    return tokens


# ---------------------------------------------------------------------------
# Recursive-descent parser
# ---------------------------------------------------------------------------

class _ExprParser:
    def __init__(self, tokens: list[tuple[str, str]]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> tuple[str, str] | None:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def peek_at(self, offset: int) -> tuple[str, str] | None:
        idx = self.pos + offset
        if idx < len(self.tokens):
            return self.tokens[idx]
        return None

    def consume(self, expected_kind: str | None = None) -> tuple[str, str]:
        tok = self.peek()
        if tok is None:
            raise ValueError("Unexpected end of expression")
        if expected_kind and tok[0] != expected_kind:
            raise ValueError(f"Expected {expected_kind}, got {tok[0]} ({tok[1]!r})")
        self.pos += 1
        return tok

    def parse(self) -> Expr:
        return self._parse_or()

    def _parse_or(self) -> Expr:
        left = self._parse_and()
        while (
            self.peek()
            and self.peek()[0] == "IDENT"
            and self.peek()[1].lower() == "or"
        ):
            self.consume("IDENT")
            right = self._parse_and()
            left = BinOp(left=left, op="OR", right=right)
        return left

    def _parse_and(self) -> Expr:
        left = self._parse_comparison()
        while (
            self.peek()
            and self.peek()[0] == "IDENT"
            and self.peek()[1].lower() == "and"
        ):
            self.consume("IDENT")
            right = self._parse_comparison()
            left = BinOp(left=left, op="AND", right=right)
        return left

    def _parse_comparison(self) -> Expr:
        left = self._parse_add()
        tok = self.peek()
        if tok and tok[0] in _COMPARISON_OPS:
            op_tok = self.consume()
            op = _COMPARISON_OPS[op_tok[0]]
            # Handle '==' (two EQ tokens) — treat same as '='
            if op == "=" and self.peek() and self.peek()[0] == "EQ":
                self.consume("EQ")  # consume second '='
                op = "="
            right = self._parse_add()
            return BinOp(left=left, op=op, right=right)
        # Postfix: 'is null' / 'is not null' — binds tighter than AND/OR
        if tok and tok[0] == "IDENT" and tok[1].lower() == "is":
            self.consume("IDENT")  # 'is'
            nxt = self.peek()
            if nxt and nxt[0] == "IDENT" and nxt[1].lower() == "not":
                self.consume("IDENT")  # 'not'
                nxt2 = self.peek()
                if nxt2 and nxt2[0] == "IDENT" and nxt2[1].lower() == "null":
                    self.consume("IDENT")  # 'null'
                    return UnaryExpr(op="is_not_null", expr=left)
                raise ValueError("Expected 'null' after 'is not'")
            if nxt and nxt[0] == "IDENT" and nxt[1].lower() == "null":
                self.consume("IDENT")  # 'null'
                return UnaryExpr(op="is_null", expr=left)
            raise ValueError("Expected 'null' or 'not null' after 'is'")
        # 'in' list literal
        if tok and tok[0] == "IDENT" and tok[1].lower() == "in":
            self.consume("IDENT")  # 'in'
            list_lit = self._parse_list_lit()
            return InExpr(expr=left, values=list_lit.values, negated=False)
        # 'not_in' list literal
        if tok and tok[0] == "IDENT" and tok[1].lower() == "not_in":
            self.consume("IDENT")  # 'not_in'
            list_lit = self._parse_list_lit()
            return InExpr(expr=left, values=list_lit.values, negated=True)
        return left

    def _parse_add(self) -> Expr:
        left = self._parse_mul()
        while self.peek() and self.peek()[0] in ("PLUS", "MINUS"):
            op_tok = self.consume()
            op = "+" if op_tok[0] == "PLUS" else "-"
            right = self._parse_mul()
            left = BinOp(left=left, op=op, right=right)
        return left

    def _parse_mul(self) -> Expr:
        left = self._parse_unary()
        while self.peek() and self.peek()[0] in ("STAR", "SLASH"):
            op_tok = self.consume()
            op = "*" if op_tok[0] == "STAR" else "/"
            right = self._parse_unary()
            left = BinOp(left=left, op=op, right=right)
        return left

    def _parse_unary(self) -> Expr:
        tok = self.peek()
        if tok is None:
            raise ValueError("Unexpected end of expression")
        if tok[0] == "MINUS":
            self.consume("MINUS")
            return UnaryExpr(op="-", expr=self._parse_unary())
        if tok[0] == "IDENT" and tok[1].lower() == "not":
            self.consume("IDENT")
            return UnaryExpr(op="not", expr=self._parse_unary())
        return self._parse_primary()

    def _parse_list_lit(self) -> ListLit:
        """Parse a bracketed list literal: [ expr, expr, ... ]"""
        self.consume("LBRACKET")
        values = []
        if self.peek() and self.peek()[0] != "RBRACKET":
            values.append(self.parse())
            while self.peek() and self.peek()[0] == "COMMA":
                self.consume("COMMA")
                values.append(self.parse())
        self.consume("RBRACKET")
        return ListLit(values=values)

    def _parse_primary(self) -> Expr:
        tok = self.peek()
        if tok is None:
            raise ValueError("Unexpected end of expression in primary")

        # List literal: [ ... ]
        if tok[0] == "LBRACKET":
            return self._parse_list_lit()

        # Duration literal: 7d, 4w
        if tok[0] == "DURATION":
            self.consume("DURATION")
            amount = int(tok[1][:-1])
            unit = tok[1][-1]
            return DurationLit(amount=amount, unit=unit)

        # Parenthesised sub-expression
        if tok[0] == "LPAREN":
            self.consume("LPAREN")
            expr = self.parse()
            self.consume("RPAREN")
            return self._parse_method_calls(expr)

        # Number literal
        if tok[0] == "NUMBER":
            self.consume("NUMBER")
            val: str | int | float = float(tok[1]) if "." in tok[1] else int(tok[1])
            return Lit(value=val)

        # String literal
        if tok[0] == "STRING":
            self.consume("STRING")
            return Lit(value=tok[1][1:-1])

        # Star literal (bare * for COUNT(*))
        if tok[0] == "STAR":
            self.consume("STAR")
            return Lit(value="*")

        # Identifier: could be keyword literal, CASE WHEN, agg/scalar func, or field ref
        if tok[0] == "IDENT":
            name_lower = tok[1].lower()

            # Keyword literals
            if name_lower == "true":
                self.consume("IDENT")
                return Lit(value=True)
            if name_lower == "false":
                self.consume("IDENT")
                return Lit(value=False)
            if name_lower in ("null", "none"):
                self.consume("IDENT")
                return Lit(value=None)

            # CASE WHEN ... THEN ... [ELSE ...] END
            if name_lower == "case":
                return self._parse_case_when()

            # Function call: IDENT followed by LPAREN
            nxt = self.peek_at(1)
            if nxt and nxt[0] == "LPAREN":
                self.consume("IDENT")   # function name
                self.consume("LPAREN")

                if name_lower in _AGG_FUNC_NAMES:
                    agg_func = AggFunc(name_lower)
                    if self.peek() and self.peek()[0] == "RPAREN":
                        # count() → COUNT(*)
                        self.consume("RPAREN")
                        return AggExpr(func=agg_func, expr=Lit(value="*"))
                    inner = self.parse()
                    self.consume("RPAREN")
                    return AggExpr(func=agg_func, expr=inner)
                else:
                    # Scalar function call (positional and/or keyword args)
                    args: list[Expr] = []
                    kwargs: dict[str, Expr] = {}
                    if self.peek() and self.peek()[0] != "RPAREN":
                        self._parse_arg_or_kwarg(args, kwargs)
                        while self.peek() and self.peek()[0] == "COMMA":
                            self.consume("COMMA")
                            self._parse_arg_or_kwarg(args, kwargs)
                    self.consume("RPAREN")
                    func_expr = FuncCallExpr(name=name_lower, args=args, kwargs=kwargs)
                    # Check for .over() postfix — window function syntax
                    if (
                        self.peek() and self.peek()[0] == "DOT"
                        and self.peek_at(1) and self.peek_at(1)[0] == "IDENT"
                        and self.peek_at(1)[1].lower() == "over"
                    ):
                        window_expr = self._parse_over(func_expr)
                        # After .over(), check for further method calls
                        return self._parse_method_calls(window_expr)
                    # Check for other method calls postfix
                    return self._parse_method_calls(func_expr)

            # Field reference: IDENT ('.' IDENT)*
            # But stop at DOT IDENT LPAREN (method call) — unless it's 'over'
            parts = [tok[1]]
            self.consume("IDENT")
            while self.peek() and self.peek()[0] == "DOT":
                # Look ahead: if next is IDENT followed by LPAREN, it's a method call
                next_tok = self.peek_at(1)
                if (
                    next_tok and next_tok[0] == "IDENT"
                    and self.peek_at(2) and self.peek_at(2)[0] == "LPAREN"
                ):
                    # It's a method call — stop collecting field parts
                    break
                self.consume("DOT")
                nxt_part = self.consume("IDENT")
                parts.append(nxt_part[1])
            field_ref = FieldRef(parts=parts)
            return self._parse_method_calls(field_ref)

        raise ValueError(f"Unexpected token {tok[0]} ({tok[1]!r}) in expression")

    def _parse_method_calls(self, receiver: Expr) -> Expr:
        """Parse zero or more .method(args) postfix calls on a receiver.

        Specifically skips .over(...) — that remains as WindowExpr and is
        handled separately in _parse_over called from the function-call branch.
        """
        while (
            self.peek() and self.peek()[0] == "DOT"
            and self.peek_at(1) and self.peek_at(1)[0] == "IDENT"
            and self.peek_at(2) and self.peek_at(2)[0] == "LPAREN"
        ):
            method_name = self.peek_at(1)[1]
            # .over() belongs to WindowExpr — skip
            if method_name.lower() == "over":
                break
            self.consume("DOT")
            self.consume("IDENT")   # method name
            self.consume("LPAREN")
            args: list[Expr] = []
            if self.peek() and self.peek()[0] != "RPAREN":
                args.append(self.parse())
                while self.peek() and self.peek()[0] == "COMMA":
                    self.consume("COMMA")
                    args.append(self.parse())
            self.consume("RPAREN")
            receiver = MethodCallExpr(expr=receiver, method=method_name, args=args)
        return receiver

    def _parse_arg_or_kwarg(
        self,
        args: list[Expr],
        kwargs: dict[str, Expr],
    ) -> None:
        """Parse one function argument — positional or keyword (name=value)."""
        # Keyword arg: bare IDENT immediately followed by EQ (=)
        if (
            self.peek() and self.peek()[0] == "IDENT"
            and self.peek_at(1) and self.peek_at(1)[0] == "EQ"
        ):
            name_tok = self.consume("IDENT")
            self.consume("EQ")
            kwargs[name_tok[1]] = self.parse()
        else:
            args.append(self.parse())

    def _parse_over(self, func_expr: FuncCallExpr) -> WindowExpr:
        """Parse .over(partition_by..., order_by) postfix after a window function call.

        Convention: all positional args to .over() except the last are partition_by;
        the last arg is order_by.
        """
        self.consume("DOT")
        self.consume("IDENT")   # 'over'
        self.consume("LPAREN")

        over_args: list[Expr] = []
        if self.peek() and self.peek()[0] != "RPAREN":
            over_args.append(self.parse())
            while self.peek() and self.peek()[0] == "COMMA":
                self.consume("COMMA")
                over_args.append(self.parse())
        self.consume("RPAREN")

        if len(over_args) < 2:
            raise ValueError(
                f".over() requires at least 2 arguments (partition_by..., order_by), "
                f"got {len(over_args)}"
            )

        arg = func_expr.args[0] if func_expr.args else None
        return WindowExpr(
            func=func_expr.name,
            arg=arg,
            partition_by=over_args[:-1],
            order_by=[over_args[-1]],
        )

    def _parse_case_when(self) -> CaseWhenExpr:
        self.consume("IDENT")  # 'case'
        nxt = self.peek()
        if not (nxt and nxt[0] == "IDENT" and nxt[1].lower() == "when"):
            raise ValueError("Expected 'when' after 'case'")
        self.consume("IDENT")  # 'when'

        when_expr = self.parse()

        nxt = self.peek()
        if not (nxt and nxt[0] == "IDENT" and nxt[1].lower() == "then"):
            raise ValueError("Expected 'then' after case-when condition")
        self.consume("IDENT")  # 'then'

        then_expr = self.parse()

        else_expr = None
        nxt = self.peek()
        if nxt and nxt[0] == "IDENT" and nxt[1].lower() == "else":
            self.consume("IDENT")  # 'else'
            else_expr = self.parse()
            nxt = self.peek()

        if not (nxt and nxt[0] == "IDENT" and nxt[1].lower() == "end"):
            raise ValueError("Expected 'end' to close case expression")
        self.consume("IDENT")  # 'end'

        return CaseWhenExpr(when=when_expr, then=then_expr, else_=else_expr)


def parse_expression_str(text: str) -> Expr:
    """Parse a feature expression string into an Expr AST."""
    tokens = _tokenize_expr(text.strip())
    if not tokens:
        raise ValueError("Empty expression")
    parser = _ExprParser(tokens)
    result = parser.parse()
    if parser.peek() is not None:
        raise ValueError(f"Unexpected tokens after expression: {parser.peek()}")
    return result
