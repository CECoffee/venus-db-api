# app/utils.py
import re
from typing import Literal
from config import DEFAULT_DB_SCOPE

UNIPROT_REGEX = re.compile(
    r"^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2})$"
)
AMINO_ACID_ALPHABET = set(list("ACDEFGHIKLMNPQRSTVWY"))

def is_uniprot_like_id(s: str) -> bool:
    s = s.strip()
    return bool(UNIPROT_REGEX.match(s))

def is_amino_acid_sequence(s: str, min_len: int = 21) -> bool:
    s = s.strip().upper()
    if len(s) < min_len:
        return False
    return all(ch in AMINO_ACID_ALPHABET for ch in s)

def detect_input_mode(content: str) -> Literal["ID","SEQUENCE","TEXT"]:
    if is_uniprot_like_id(content):
        return "ID"
    if is_amino_acid_sequence(content):
        return "SEQUENCE"
    return "TEXT"

def validate_sequence_chars(seq: str) -> bool:
    seq = seq.strip().upper()
    return all(ch in AMINO_ACID_ALPHABET for ch in seq)

