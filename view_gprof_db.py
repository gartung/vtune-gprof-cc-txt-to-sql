#!/usr/bin/env python3
"""
Web-based viewer for gprof SQLite databases with clickable index links.
"""
import argparse
import sqlite3
import html
from flask import Flask, request, render_template_string
import sys

app = Flask(__name__)

# Global variable to store database path
DB_PATH = None
TABLE_NAME = "gprof_cc"

# HTML template with embedded CSS
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Gprof Database Viewer</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            color: #333;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }
        h2 {
            color: #555;
            margin-top: 30px;
        }
        .controls {
            background-color: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .controls label {
            margin-right: 10px;
            font-weight: bold;
        }
        .controls input, .controls select {
            padding: 5px;
            margin-right: 15px;
        }
        .controls button {
            padding: 6px 15px;
            background-color: #4CAF50;
            color: white;
            border: none;
            border-radius: 3px;
            cursor: pointer;
        }
        .controls button:hover {
            background-color: #45a049;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        th {
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        td {
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .parent-row {
            background-color: #e8f5e9;
            font-weight: bold;
        }
        .child-row {
            background-color: #fff3e0;
        }
        .index-link {
            color: #1976D2;
            text-decoration: none;
            font-weight: bold;
        }
        .index-link:hover {
            text-decoration: underline;
            color: #0D47A1;
        }
        .breadcrumb {
            background-color: white;
            padding: 10px 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .breadcrumb a {
            color: #1976D2;
            text-decoration: none;
            margin-right: 5px;
        }
        .breadcrumb a:hover {
            text-decoration: underline;
        }
        .stats {
            background-color: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stats span {
            margin-right: 20px;
            font-weight: bold;
        }
        .no-data {
            padding: 20px;
            text-align: center;
            background-color: white;
            border-radius: 5px;
        }
        .right-align {
            text-align: right;
        }
    </style>
</head>
<body>
    <h1>Gprof Database Viewer</h1>
    
    {% if breadcrumb %}
    <div class="breadcrumb">
        <a href="/">Home</a> / {{ breadcrumb|safe }}
    </div>
    {% endif %}
    
    <div class="controls">
        <form method="get" style="display: inline;">
            <label>View:</label>
            <select name="view" onchange="this.form.submit()">
                <option value="all" {% if view == 'all' %}selected{% endif %}>All Rows</option>
                <option value="parents" {% if view == 'parents' %}selected{% endif %}>Parents Only</option>
                <option value="top_cpu" {% if view == 'top_cpu' %}selected{% endif %}>Top CPU Time</option>
            </select>
            
            <label>Limit:</label>
            <input type="number" name="limit" value="{{ limit }}" min="10" max="10000" style="width: 80px;">
            
            <label>Search Name:</label>
            <input type="text" name="search" value="{{ search }}" placeholder="Function name...">
            
            <input type="hidden" name="index" value="{{ current_index }}">
            
            <button type="submit">Apply</button>
        </form>
    </div>
    
    <div class="stats">
        <span>Total Rows: {{ total_rows }}</span>
        <span>Showing: {{ showing_rows }}</span>
    </div>
    
    {% if title %}
    <h2>{{ title }}</h2>
    {% endif %}
    
    {% if rows %}
    <table>
        <thead>
            <tr>
                <th>Index</th>
                <th class="right-align">% CPU Time</th>
                <th class="right-align">CPU Self</th>
                <th class="right-align">CPU Children</th>
                <th class="right-align">CPU Total</th>
                <th>Name</th>
                <th>Parent</th>
            </tr>
        </thead>
        <tbody>
            {% for row in rows %}
            <tr class="{% if row.is_parent %}parent-row{% elif row.parent_index %}child-row{% endif %}">
                <td>
                    {% if row.index %}
                    <a href="?index={{ row.index }}&view=children" class="index-link">[{{ row.index }}]</a>
                    {% endif %}
                </td>
                <td class="right-align">{{ "%.2f"|format(row.cpu_time_total) if row.cpu_time_total else "" }}</td>
                <td class="right-align">{{ "%.6f"|format(row.cpu_time_self) if row.cpu_time_self else "" }}</td>
                <td class="right-align">{{ "%.6f"|format(row.cpu_time_children) if row.cpu_time_children else "" }}</td>
                <td class="right-align">{{ "%.6f"|format(row.cpu_sum) if row.cpu_sum else "" }}</td>
                <td>{{ row.name }}</td>
                <td>
                    {% if row.parent_index %}
                    <a href="?index={{ row.parent_index }}&view=children" class="index-link">[{{ row.parent_index }}]</a>
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
    {% else %}
    <div class="no-data">No data found.</div>
    {% endif %}
</body>
</html>
"""


@app.route('/')
def index():
    """Main view handler."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get parameters
    view = request.args.get('view', 'all')
    limit = int(request.args.get('limit', 100))
    search = request.args.get('search', '')
    current_index = request.args.get('index', '')
    
    # Build query based on view type
    where_clauses = []
    params = []
    title = ""
    breadcrumb = ""
    
    if current_index:
        # Show specific index and its children
        where_clauses.append("(\"index\" = ? OR parent_index = ?)")
        params.extend([int(current_index), int(current_index)])
        title = f"Index [{current_index}] and its children"
        breadcrumb = f'Index <a href="?index={current_index}&view=children">[{current_index}]</a>'
        view = 'children'
    elif view == 'parents':
        where_clauses.append("parent_index IS NULL")
        title = "Parent Rows Only"
    elif view == 'top_cpu':
        where_clauses.append("cpu_time_total IS NOT NULL")
        title = f"Top {limit} by CPU Time"
    
    if search:
        where_clauses.append("name LIKE ?")
        params.append(f"%{search}%")
        if title:
            title += f" (filtered by '{search}')"
        else:
            title = f"Results for '{search}'"
    
    # Build WHERE clause
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)
    
    # Build ORDER BY clause
    if view == 'top_cpu':
        order_sql = "ORDER BY cpu_time_total DESC"
    elif current_index:
        order_sql = "ORDER BY CASE WHEN \"index\" = ? THEN 0 ELSE 1 END, ROWID"
        params.insert(0, int(current_index))
    else:
        order_sql = "ORDER BY ROWID"
    
    # Get total count
    count_sql = f'SELECT COUNT(*) FROM "{TABLE_NAME}" {where_sql}'
    cursor.execute(count_sql, params if not current_index else params[1:] if params else [])
    total_rows = cursor.fetchone()[0]
    
    # Get rows
    sql = f'''
        SELECT 
            "index",
            cpu_time_total,
            cpu_time_self,
            cpu_time_children,
            COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0) as cpu_sum,
            name,
            parent_index,
            CASE WHEN parent_index IS NULL AND "index" IS NOT NULL THEN 1 ELSE 0 END as is_parent
        FROM "{TABLE_NAME}"
        {where_sql}
        {order_sql}
        LIMIT ?
    '''
    params.append(limit)
    
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    
    conn.close()
    
    return render_template_string(
        HTML_TEMPLATE,
        rows=rows,
        view=view,
        limit=limit,
        search=search,
        current_index=current_index,
        total_rows=total_rows,
        showing_rows=len(rows),
        title=title,
        breadcrumb=breadcrumb
    )


def main():
    global DB_PATH, TABLE_NAME
    
    parser = argparse.ArgumentParser(description="Web-based viewer for gprof SQLite databases")
    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument("--table", default="gprof_cc", help="Table name (default: gprof_cc)")
    parser.add_argument("--port", type=int, default=5000, help="Port to run the web server (default: 5000)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    
    args = parser.parse_args()
    
    # Verify database exists
    try:
        conn = sqlite3.connect(args.db_path)
        cursor = conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{args.table}"')
        count = cursor.fetchone()[0]
        conn.close()
        print(f"Database loaded: {count} rows in table '{args.table}'")
    except Exception as e:
        print(f"Error loading database: {e}", file=sys.stderr)
        sys.exit(1)
    
    DB_PATH = args.db_path
    TABLE_NAME = args.table
    
    print(f"\nStarting web server at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop\n")
    
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
