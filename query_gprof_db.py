#!/usr/bin/env python3
"""
Query and analyze gprof-cc SQLite database.
Provides various useful analytics on profiling data.
"""
import argparse
import sqlite3
import sys
from typing import List, Tuple


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def print_table(headers: List[str], rows: List[Tuple], widths: List[int] = None):
    """Print results in a formatted table."""
    if not rows:
        print("(No results)")
        return
    
    # Auto-calculate widths if not provided
    if widths is None:
        widths = [len(h) for h in headers]
        for row in rows:
            for i, val in enumerate(row):
                widths[i] = max(widths[i], len(str(val)))
    
    # Print header
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("-" * len(header_line))
    
    # Print rows
    for row in rows:
        print(" | ".join(str(val).ljust(widths[i]) for i, val in enumerate(row)))


def query_top_cpu_consumers(conn: sqlite3.Connection, limit: int = 20):
    """Find functions with highest total CPU time."""
    print_section(f"Top {limit} CPU Time Consumers")
    
    cursor = conn.execute("""
        SELECT 
            name,
            ROUND(cpu_time_total, 2) as pct_total,
            ROUND(cpu_time_self, 2) as time_self,
            ROUND(cpu_time_children, 2) as time_children,
            "index"
        FROM gprof_cc
        WHERE cpu_time_total IS NOT NULL
        ORDER BY cpu_time_total DESC
        LIMIT ?
    """, (limit,))
    
    rows = cursor.fetchall()
    print_table(
        ["Function Name", "% Total", "Self Time", "Children Time", "Index"],
        rows,
        [50, 10, 12, 15, 8]
    )


def query_high_self_time(conn: sqlite3.Connection, threshold: float = 0.5, limit: int = 15):
    """Find functions spending significant time in their own code (not children)."""
    print_section(f"Functions with Self-Time > {threshold} (Top {limit})")
    
    cursor = conn.execute("""
        SELECT 
            name,
            ROUND(cpu_time_self, 2) as time_self,
            ROUND(cpu_time_total, 2) as pct_total,
            ROUND(cpu_time_children, 2) as time_children,
            ROUND(100.0 * cpu_time_self / NULLIF(cpu_time_total, 0), 1) as self_pct
        FROM gprof_cc
        WHERE cpu_time_self > ?
        ORDER BY cpu_time_self DESC
        LIMIT ?
    """, (threshold, limit))
    
    rows = cursor.fetchall()
    print_table(
        ["Function Name", "Self Time", "% Total", "Children Time", "Self %"],
        rows,
        [50, 12, 10, 15, 8]
    )


def search_functions(conn: sqlite3.Connection, pattern: str, limit: int = 20):
    """Search for functions by name pattern."""
    print_section(f"Functions matching '{pattern}' (Top {limit})")
    
    cursor = conn.execute("""
        SELECT 
            name,
            ROUND(cpu_time_total, 2) as pct_total,
            ROUND(cpu_time_self, 2) as time_self,
            "index"
        FROM gprof_cc
        WHERE name LIKE ?
        ORDER BY cpu_time_total DESC NULLS LAST
        LIMIT ?
    """, (f"%{pattern}%", limit))
    
    rows = cursor.fetchall()
    print_table(
        ["Function Name", "% Total", "Self Time", "Index"],
        rows,
        [55, 10, 12, 8]
    )


def query_statistics(conn: sqlite3.Connection):
    """Show statistical summary of profiling data."""
    print_section("Statistical Summary")
    
    cursor = conn.execute("""
        SELECT 
            COUNT(*) as total_entries,
            COUNT(CASE WHEN cpu_time_total IS NOT NULL THEN 1 END) as with_timing,
            ROUND(AVG(cpu_time_total), 4) as avg_pct_total,
            ROUND(MAX(cpu_time_total), 2) as max_pct_total,
            ROUND(SUM(cpu_time_self), 2) as total_self_time,
            ROUND(AVG(cpu_time_self), 4) as avg_self_time
        FROM gprof_cc
    """)
    
    row = cursor.fetchone()
    labels = [
        "Total Entries:",
        "Entries with Timing:",
        "Avg % Total:",
        "Max % Total:",
        "Sum Self Time:",
        "Avg Self Time:"
    ]
    
    for label, value in zip(labels, row):
        print(f"{label:25} {value}")


def query_cycles(conn: sqlite3.Connection):
    """Find cycle-related entries."""
    print_section("Call Cycles Detected")
    
    cursor = conn.execute("""
        SELECT 
            name,
            ROUND(cpu_time_total, 2) as pct_total,
            ROUND(cpu_time_self, 2) as time_self,
            "index"
        FROM gprof_cc
        WHERE name LIKE '%cycle%'
        ORDER BY cpu_time_total DESC NULLS LAST
        LIMIT 20
    """)
    
    rows = cursor.fetchall()
    if rows:
        print_table(
            ["Function Name", "% Total", "Self Time", "Index"],
            rows,
            [55, 10, 12, 8]
        )
    else:
        print("(No cycles detected)")


def query_expensive_children(conn: sqlite3.Connection, limit: int = 15):
    """Find functions whose children consume most time (coordination/framework functions)."""
    print_section(f"Functions with Expensive Children (Top {limit})")
    
    cursor = conn.execute("""
        SELECT 
            name,
            ROUND(cpu_time_children, 2) as time_children,
            ROUND(cpu_time_self, 2) as time_self,
            ROUND(cpu_time_total, 2) as pct_total,
            ROUND(100.0 * cpu_time_children / NULLIF(cpu_time_total, 0), 1) as children_pct
        FROM gprof_cc
        WHERE cpu_time_children IS NOT NULL
        ORDER BY cpu_time_children DESC
        LIMIT ?
    """, (limit,))
    
    rows = cursor.fetchall()
    print_table(
        ["Function Name", "Children Time", "Self Time", "% Total", "Children %"],
        rows,
        [50, 15, 12, 10, 12]
    )


def query_parent_and_children(conn: sqlite3.Connection, pattern: str):
    """Display parent functions matching pattern along with their children."""
    print_section(f"Parents matching '{pattern}' with their children")
    
    # Check if parent_index column exists
    cursor = conn.execute("PRAGMA table_info(gprof_cc)")
    columns = {row[1] for row in cursor.fetchall()}
    has_parent_index = 'parent_index' in columns
    
    # Find parent functions matching the pattern
    if has_parent_index:
        parent_query = """
            SELECT DISTINCT "index", name, 
                   ROUND(cpu_time_total, 2) as pct_total,
                   ROUND(cpu_time_self, 2) as time_self,
                   ROUND(cpu_time_children, 2) as time_children,
                   ROUND(COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0), 2) as total_time
            FROM gprof_cc
            WHERE parent_index IS NULL 
              AND name LIKE ?
            ORDER BY cpu_time_total DESC NULLS LAST
        """
    else:
        parent_query = """
            SELECT DISTINCT "index", name,
                   ROUND(cpu_time_total, 2) as pct_total,
                   ROUND(cpu_time_self, 2) as time_self,
                   ROUND(cpu_time_children, 2) as time_children,
                   ROUND(COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0), 2) as total_time
            FROM gprof_cc
            WHERE "index" IS NOT NULL 
              AND "index" = "index_1"
              AND name LIKE ?
            ORDER BY cpu_time_total DESC NULLS LAST
        """
    
    cursor = conn.execute(parent_query, (f"%{pattern}%",))
    parents = cursor.fetchall()
    
    if not parents:
        print(f"(No parent functions matching '{pattern}')")
        return
    
    # Track if we displayed any parents (ones with children)
    displayed_count = 0
    
    for parent in parents:
        parent_index, parent_name, pct_total, time_self, time_children, total_time = parent
        
        # Get children of this parent first to see if we should display this parent
        if has_parent_index:
            children_query = """
                SELECT "index_1" as child_index, name,
                       ROUND(cpu_time_total, 2) as pct_total,
                       ROUND(cpu_time_self, 2) as time_self,
                       ROUND(cpu_time_children, 2) as time_children,
                       ROUND(COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0), 2) as total_time
                FROM gprof_cc
                WHERE parent_index = ?
                ORDER BY COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0) DESC
            """
        else:
            children_query = """
                SELECT "index_1" as child_index, name,
                       ROUND(cpu_time_total, 2) as pct_total,
                       ROUND(cpu_time_self, 2) as time_self,
                       ROUND(cpu_time_children, 2) as time_children,
                       ROUND(COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0), 2) as total_time
                FROM gprof_cc
                WHERE "index" = ? AND "index" != "index_1"
                ORDER BY COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0) DESC
            """
        
        cursor = conn.execute(children_query, (parent_index,))
        children = cursor.fetchall()
        
        # Only display parents that have children
        if children:
            displayed_count += 1
            print(f"\n{'─' * 70}")
            print(f"PARENT [{parent_index}]: {parent_name}")
            print(f"  % Total: {pct_total if pct_total else 'N/A'}  |  Self: {time_self if time_self else 'N/A'}  |  Children: {time_children if time_children else 'N/A'}  |  Total: {total_time}")
            print(f"{'─' * 70}")
            
            print("\nChildren:")
            print_table(
                ["Index", "Function Name", "% Total", "Self", "Children", "Total"],
                children,
                [8, 45, 10, 10, 10, 10]
            )
            print()  # Extra spacing between parent groups
    
    if displayed_count == 0:
        print(f"(No parent functions matching '{pattern}' have children)")


def main():
    parser = argparse.ArgumentParser(
        description="Query and analyze gprof-cc SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s db.sqlite --all
  %(prog)s db.sqlite --top 10
  %(prog)s db.sqlite --search edm::
  %(prog)s db.sqlite --stats --cycles
        """
    )
    parser.add_argument("db_path", help="Path to SQLite database")
    parser.add_argument("--all", action="store_true", help="Run all standard queries")
    parser.add_argument("--top", type=int, metavar="N", help="Show top N CPU consumers")
    parser.add_argument("--self-time", type=float, metavar="THRESHOLD", help="Show functions with self-time > threshold")
    parser.add_argument("--search", type=str, metavar="PATTERN", help="Search for functions by name pattern")
    parser.add_argument("--stats", action="store_true", help="Show statistical summary")
    parser.add_argument("--cycles", action="store_true", help="Show call cycles")
    parser.add_argument("--children", type=int, metavar="N", help="Show top N functions by children time")
    parser.add_argument("--parent", type=str, metavar="PATTERN", help="Show parent(s) matching pattern with their children")
    parser.add_argument("--table", type=str, default="gprof_cc", help="Table name (default: gprof_cc)")
    
    args = parser.parse_args()
    
    try:
        conn = sqlite3.connect(args.db_path)
    except sqlite3.Error as e:
        print(f"Error opening database: {e}", file=sys.stderr)
        return 1
    
    # Set table name globally (simple approach for this script)
    global TABLE_NAME
    TABLE_NAME = args.table
    
    try:
        # If no specific query specified, show defaults
        if not any([args.all, args.top, args.self_time, args.search, 
                   args.stats, args.cycles, args.children, args.parent]):
            args.all = True
        
        if args.all:
            query_statistics(conn)
            query_top_cpu_consumers(conn, limit=15)
            query_high_self_time(conn, threshold=0.5, limit=10)
            query_expensive_children(conn, limit=10)
            query_cycles(conn)
        else:
            if args.stats:
                query_statistics(conn)
            if args.top:
                query_top_cpu_consumers(conn, limit=args.top)
            if args.self_time is not None:
                query_high_self_time(conn, threshold=args.self_time, limit=15)
            if args.children:
                query_expensive_children(conn, limit=args.children)
            if args.search:
                search_functions(conn, args.search)
            if args.parent:
                query_parent_and_children(conn, args.parent)
            if args.cycles:
                query_cycles(conn)
        
    finally:
        conn.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
