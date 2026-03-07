import re

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-^]{1,15}$")


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def is_valid_symbol(symbol: str) -> bool:
    return bool(SYMBOL_PATTERN.fullmatch(symbol))


def normalize_query(query: str) -> str:
    return query.strip()
