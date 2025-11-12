#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sqlite3
from typing import List, Tuple, Dict, Any


def sanitize_column(name: str, used: set, index: int) -> str:
    # Normalize whitespace and case
    if name is None:
        name = ""
    name = name.strip()
    # Fallback if empty
    if not name:
        name = f"col_{index+1}"
    # Replace spaces and invalid chars
    name = name.lower()
    name = name.replace(" ", "_")
    name = re.sub(r"[^a-z0-9_]", "_", name)
    # Avoid leading digits
    if re.match(r"^[0-9]", name):
        name = f"c_{name}"
    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name).strip("_") or f"col_{index+1}"
    # Ensure uniqueness
    base = name
    suffix = 1
    while name in used:
        name = f"{base}_{suffix}"
        suffix += 1
    used.add(name)
    return name


def infer_headers(reader: csv.reader) -> List[str]:
    try:
        raw_headers = next(reader)
    except StopIteration:
        return []
    used = set()
    headers = [sanitize_column(h, used, i) for i, h in enumerate(raw_headers)]
    return headers


def extract_index_number(value: str) -> int:
    """Extract numeric value from bracketed index like '[196]'."""
    if not value:
        return None
    match = re.search(r'\[(\d+)\]', value)
    return int(match.group(1)) if match else None


def infer_column_types(headers: List[str]) -> Dict[str, str]:
    """Map column names to SQL types based on header semantics."""
    types = {}
    for h in headers:
        lower = h.lower()
        # CPU time columns should be REAL (floating point)
        if any(x in lower for x in ["cpu_time", "time_self", "time_children", "time_total", "% cpu"]):
            types[h] = "REAL"
        # Index columns containing numbers in brackets - store extracted value as INTEGER
        elif "index" in lower:
            types[h] = "INTEGER"
        else:
            types[h] = "TEXT"
    return types


def create_table(conn: sqlite3.Connection, table: str, headers: List[str], column_types: Dict[str, str]):
    # Quote identifiers safely with double-quotes
    # Add parent_index column to track parent-child relationships
    cols = ", ".join(f'"{h}" {column_types.get(h, "TEXT")}' for h in headers)
    cols += ', "parent_index" INTEGER'
    sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({cols});'
    conn.execute(sql)


def is_empty_row(row: List[str]) -> bool:
    """Check if a row is empty (all fields are empty or whitespace)."""
    return all(not field or not field.strip() for field in row)


def is_parent_row(row: List[str]) -> bool:
    """Check if a row is a parent (has index in brackets in both first and last columns)."""
    if len(row) < 2:
        return False
    first_col = row[0].strip() if row[0] else ""
    last_col = row[-1].strip() if row[-1] else ""
    # Check if both first and last columns have values in brackets
    return (bool(re.match(r'^\[\d+\]$', first_col)) and 
            bool(re.match(r'^\[\d+\]$', last_col)))


def insert_rows(conn: sqlite3.Connection, table: str, headers: List[str], column_types: Dict[str, str], rows_iter, batch_size: int = 1000):
    # Add parent_index column to placeholders and columns
    placeholders = ", ".join(["?"] * (len(headers) + 1))
    cols_quoted = ", ".join([f'"{h}"' for h in headers]) + ', "parent_index"'
    sql = f'INSERT INTO "{table}" ({cols_quoted}) VALUES ({placeholders})'
    batch: List[Tuple[Any, ...]] = []
    count = 0
    current_parent_index = None
    
    for row in rows_iter:
        # Pad or trim row to match headers length
        if len(row) < len(headers):
            row = row + ["" for _ in range(len(headers) - len(row))]
        elif len(row) > len(headers):
            row = row[:len(headers)]
        
        # Check if this is an empty row (signals end of children)
        if is_empty_row(row):
            current_parent_index = None
            # Skip inserting empty rows
            continue
        
        # Check if this is a parent row
        if is_parent_row(row):
            # Extract the parent index from the last column
            current_parent_index = extract_index_number(row[-1])
        
        # Convert values based on column types
        converted_row = []
        for i, (h, val) in enumerate(zip(headers, row)):
            col_type = column_types.get(h, "TEXT")
            if not val or not val.strip():
                converted_row.append(None)
            elif col_type == "REAL":
                try:
                    converted_row.append(float(val))
                except (ValueError, TypeError):
                    converted_row.append(None)
            elif col_type == "INTEGER":
                # Try to extract index number from brackets
                idx_num = extract_index_number(val)
                converted_row.append(idx_num)
            else:
                converted_row.append(val)
        
        # Add parent_index as the last value
        # For parent rows, parent_index is NULL
        # For child rows, parent_index is the current parent's index
        if is_parent_row(row):
            converted_row.append(None)  # Parent rows have no parent
        else:
            converted_row.append(current_parent_index)  # Child rows reference their parent
        
        batch.append(tuple(converted_row))
        if len(batch) >= batch_size:
            conn.executemany(sql, batch)
            conn.commit()
            count += len(batch)
            batch.clear()
    if batch:
        conn.executemany(sql, batch)
        conn.commit()
        count += len(batch)
    return count


def main():
    parser = argparse.ArgumentParser(description="Convert a semicolon-delimited gprof-cc report to a SQLite database.")
    parser.add_argument("csv_path", help="Path to the input CSV (semicolon-delimited)")
    parser.add_argument("--db", dest="db_path", help="Path to output SQLite DB (default: csv filename with .sqlite)")
    parser.add_argument("--table", dest="table", default="gprof_cc", help="Destination table name (default: gprof_cc)")
    parser.add_argument("--delimiter", dest="delimiter", default=";", help="Field delimiter (default: ;)")
    parser.add_argument("--encoding", dest="encoding", default="utf-8", help="File encoding (default: utf-8)")
    parser.add_argument("--batch-size", dest="batch", type=int, default=2000, help="Batch size for inserts (default: 2000)")
    parser.add_argument("--quotechar", dest="quotechar", default='"', help='CSV quote char (default: ")')

    args = parser.parse_args()

    csv_path = os.path.abspath(args.csv_path)
    if not os.path.exists(csv_path):
        raise SystemExit(f"Input file not found: {csv_path}")

    db_path = os.path.abspath(args.db_path) if args.db_path else os.path.splitext(csv_path)[0] + ".sqlite"
    table = args.table

    # Create DB directory if needed
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn, open(csv_path, "r", encoding=args.encoding, newline="") as f:
        reader = csv.reader(f, delimiter=args.delimiter, quotechar=args.quotechar)
        headers = infer_headers(reader)
        if not headers:
            raise SystemExit("CSV appears to be empty; no headers found.")
        
        # Infer column types from header names
        column_types = infer_column_types(headers)
        
        create_table(conn, table, headers, column_types)
        total = insert_rows(conn, table, headers, column_types, reader, batch_size=args.batch)
        
        # Create useful indexes
        print(f"Creating indexes...")
        # Index on name column for fast lookups
        try:
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{table}_name_idx" ON "{table}"(name)')
        except sqlite3.DatabaseError:
            pass
        
        # Index on first index column if it exists
        if "index" in headers:
            try:
                conn.execute(f'CREATE INDEX IF NOT EXISTS "{table}_index_idx" ON "{table}"("index")')
            except sqlite3.DatabaseError:
                pass
        
        # Index on cpu_time_total for finding top entries
        if "cpu_time_total" in headers:
            try:
                conn.execute(f'CREATE INDEX IF NOT EXISTS "{table}_cpu_total_idx" ON "{table}"(cpu_time_total DESC)')
            except sqlite3.DatabaseError:
                pass
        
        # Index on parent_index for finding children
        try:
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{table}_parent_idx" ON "{table}"(parent_index)')
        except sqlite3.DatabaseError:
            pass
        
        conn.commit()

    print(f"Done. Inserted {total} rows into {db_path}, table '{table}'.")
    print(f"Column types: {', '.join(f'{h}:{column_types[h]}' for h in headers)}")


if __name__ == "__main__":
    main()
