#!/usr/bin/env python3
"""
CGI-based viewer for gprof SQLite databases with clickable index links.
No server needed - just place in your web server's cgi-bin directory.

Usage: view_gprof_db.cgi?db=/path/to/database.sqlite
"""
import cgi
import cgitb
import sqlite3
import os
import sys
from urllib.parse import urlencode

# Enable CGI error reporting
cgitb.enable()

# Configuration
TABLE_NAME = "gprof_cc"

# HTML template
HTML_TEMPLATE = """Content-Type: text/html

<!DOCTYPE html>
<html>
<head>
    <title>Gprof Database Viewer</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 30px;
        }}
        .controls {{
            background-color: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .controls label {{
            margin-right: 10px;
            font-weight: bold;
        }}
        .controls input, .controls select {{
            padding: 5px;
            margin-right: 15px;
        }}
        .controls button {{
            padding: 6px 15px;
            background-color: #4CAF50;
            color: white;
            border: none;
            border-radius: 3px;
            cursor: pointer;
        }}
        .controls button:hover {{
            background-color: #45a049;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .parent-row {{
            background-color: #e8f5e9;
            font-weight: bold;
            cursor: pointer; /* Make entire parent row clickable */
        }}
        .child-row {{
            background-color: #fff3e0;
        }}
        .caller-row {{
            background-color: #e3f2fd; /* light blue for caller (parent's parent) */
            font-weight: bold;
            cursor: pointer;
        }}
        .index-link {{
            color: #1976D2;
            text-decoration: none;
            font-weight: bold;
        }}
        .index-link:hover {{
            text-decoration: underline;
            color: #0D47A1;
        }}
        .breadcrumb {{
            background-color: white;
            padding: 10px 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .breadcrumb a {{
            color: #1976D2;
            text-decoration: none;
            margin-right: 5px;
        }}
        .breadcrumb a:hover {{
            text-decoration: underline;
        }}
        .stats {{
            background-color: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stats span {{
            margin-right: 20px;
            font-weight: bold;
        }}
        .no-data {{
            padding: 20px;
            text-align: center;
            background-color: white;
            border-radius: 5px;
        }}
        .right-align {{
            text-align: right;
        }}
        .db-selector {{
            background-color: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
    </style>
</head>
<body>
    <h1>Gprof Database Viewer</h1>
    
    {db_selector}
    
    {breadcrumb}
    
    <div class="controls">
        <form method="get">
            {db_field}
            <label>View:</label>
            <select name="view" onchange="this.form.submit()">
                <option value="all" {selected_all}>All Rows</option>
                <option value="parents" {selected_parents}>Parents Only</option>
                <option value="top_cpu" {selected_top_cpu}>Top CPU Time</option>
            </select>
            
            <label>Limit:</label>
            <input type="number" name="limit" value="{limit}" min="10" max="10000" style="width: 80px;">
            
            <label>Search Name:</label>
            <input type="text" name="search" value="{search}" placeholder="Function name...">
            
            <input type="hidden" name="index" value="{current_index}">
            
            <button type="submit">Apply</button>
        </form>
    </div>
    
    <div class="stats">
        <span>Database: {db_name}</span>
        <span>Total Rows: {total_rows}</span>
        <span>Showing: {showing_rows}</span>
    </div>
    
    {title_section}
    
    {table_content}
</body>
</html>
"""


def html_escape(text):
    """Escape HTML special characters."""
    if text is None:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def format_name_with_links(name, db_param):
    """Format the name field, making any trailing index a clickable link."""
    if not name:
        return ""
    
    import re
    # Check if the name ends with an index like [123]
    match = re.search(r'^(.+?)\s*\[(\d+)\]$', name)
    if match:
        text_part = match.group(1)
        index_num = match.group(2)
        index_link = make_link({'index': index_num, 'view': 'children', 'db': db_param})
        return f'{html_escape(text_part)} <a href="{index_link}" class="index-link">[{index_num}]</a>'
    
    return html_escape(name)


def make_link(params):
    """Create a link with the given parameters."""
    script_name = os.environ.get('SCRIPT_NAME', 'view_gprof_db.cgi')
    if params:
        return f"{script_name}?{urlencode(params)}"
    return script_name


def get_table_columns(conn: sqlite3.Connection, table: str) -> set:
    """Return a set of column names for the given table."""
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    return {row[1] for row in cur.fetchall()}


def find_databases():
    """Find all .sqlite files in the same directory as the script."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_files = []
    for f in os.listdir(script_dir):
        if f.endswith('.sqlite'):
            full_path = os.path.join(script_dir, f)
            db_files.append((f, full_path))
    return sorted(db_files)


def validate_db_path(db_path):
    """Validate that the database path exists and is accessible."""
    if not db_path:
        return False, "No database specified. Please provide a database path using ?db=/path/to/database.sqlite"
    
    if not os.path.exists(db_path):
        return False, f"Database file not found: {db_path}"
    
    if not os.path.isfile(db_path):
        return False, f"Path is not a file: {db_path}"
    
    if not os.access(db_path, os.R_OK):
        return False, f"Database file is not readable: {db_path}"
    
    return True, None


def main():
    # Parse query parameters
    form = cgi.FieldStorage()
    
    # Get parameters
    # Default to showing parent rows
    view = form.getvalue('view', 'parents')
    limit = int(form.getvalue('limit', '100'))
    search = form.getvalue('search', '')
    current_index = form.getvalue('index', '')
    db_param = form.getvalue('db', '')
    
    # Determine database path
    db_path = None
    db_name = ""
    
    if db_param:
        # If db parameter is provided, use it
        # Support both absolute paths and relative filenames
        if os.path.isabs(db_param):
            db_path = db_param
            db_name = os.path.basename(db_path)
        else:
            # Treat as filename in script directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(script_dir, db_param)
            db_name = db_param
    
    # Validate database path
    valid, error_msg = validate_db_path(db_path)
    if not valid:
        print(HTML_TEMPLATE.format(
            db_selector="",
            breadcrumb="",
            db_field="",
            selected_all="selected",
            selected_parents="",
            selected_top_cpu="",
            limit=limit,
            search=html_escape(search),
            current_index="",
            db_name="N/A",
            total_rows=0,
            showing_rows=0,
            title_section="",
            table_content=f'<div class="no-data"><strong>Error:</strong> {html_escape(error_msg)}<br><br>Usage: ?db=/path/to/database.sqlite or ?db=filename.sqlite</div>'
        ))
        return
    
    # Find available databases in the same directory for the selector
    databases = find_databases()
    
    # Build database selector if multiple databases exist
    db_selector = ""
    if len(databases) > 1:
        options = ""
        for fname, fpath in databases:
            selected = 'selected' if fname == db_name or fpath == db_path else ''
            options += f'<option value="{html_escape(fname)}" {selected}>{html_escape(fname)}</option>'
        
        db_selector = f'''
        <div class="db-selector">
            <form method="get">
                <label>Select Database:</label>
                <select name="db" onchange="this.form.submit()">
                    {options}
                </select>
            </form>
        </div>
        '''
    
    # Connect to database
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
    except Exception as e:
        print(HTML_TEMPLATE.format(
            db_selector=db_selector,
            breadcrumb="",
            db_field=f'<input type="hidden" name="db" value="{html_escape(db_param)}">',
            selected_all="selected",
            selected_parents="",
            selected_top_cpu="",
            limit=limit,
            search=html_escape(search),
            current_index="",
            db_name=html_escape(db_name),
            total_rows=0,
            showing_rows=0,
            title_section="",
            table_content=f'<div class="no-data">Error connecting to database: {html_escape(str(e))}</div>'
        ))
        return
    
    # Build query based on view type
    where_clauses = []
    params = []
    title = ""
    breadcrumb = ""
    # Inspect table columns to determine if parent_index exists
    cols = get_table_columns(conn, TABLE_NAME)
    has_parent_index = 'parent_index' in cols
    
    if current_index:
        # Show specific index and its children
        if has_parent_index:
            where_clauses.append('(\"index\" = ? OR parent_index = ?)')
            params.extend([int(current_index), int(current_index)])
        else:
            # Fallback when parent_index is not present: rows with first index equal to the target
            where_clauses.append('\"index\" = ?')
            params.append(int(current_index))
        title = f"Index [{current_index}] and its children"
        index_link = make_link({'index': current_index, 'view': 'children', 'db': db_param})
        breadcrumb = f'<div class="breadcrumb"><a href="{make_link({"db": db_param})}">Home</a> / Index <a href="{index_link}">[{current_index}]</a></div>'
        view = 'children'
    elif view == 'parents':
        if has_parent_index:
            where_clauses.append("parent_index IS NULL")
        else:
            # Infer parent rows when parent_index column is missing
            where_clauses.append('"index" IS NOT NULL AND "index" = "index_1"')
        title = "Parent Rows Only"
    elif view == 'top_cpu':
        where_clauses.append("cpu_time_total IS NOT NULL")
        title = f"Top {limit} by CPU Time"
    
    if search:
        where_clauses.append("name LIKE ?")
        params.append(f"%{search}%")
        if title:
            title += f" (filtered by '{html_escape(search)}')"
        else:
            title = f"Results for '{html_escape(search)}'"
    
    # Build WHERE clause
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)
    
    # Build ORDER BY clause
    if current_index:
        # Parent row first, then children by CPU total (self + children)
        order_sql = 'ORDER BY CASE WHEN "index" = ? THEN 0 ELSE 1 END, cpu_sum DESC'
        params.insert(0, int(current_index))
    elif view in ('parents', 'top_cpu'):
        # Sort parents by % CPU time
        order_sql = "ORDER BY cpu_time_total DESC"
    else:
        order_sql = "ORDER BY cpu_time_total DESC"
    
    # Get total count
    count_params = params if not current_index else (params[1:] if len(params) > 0 else [])
    count_sql = f'SELECT COUNT(*) FROM "{TABLE_NAME}" {where_sql}'
    cursor.execute(count_sql, count_params)
    total_rows = cursor.fetchone()[0]
    
    # Get rows
    if has_parent_index:
        parent_select = 'parent_index'
        is_parent_sql = 'CASE WHEN parent_index IS NULL AND "index" IS NOT NULL THEN 1 ELSE 0 END'
    else:
        parent_select = 'NULL as parent_index'
        # Infer parent rows where first and last indices are equal
        is_parent_sql = 'CASE WHEN "index" IS NOT NULL AND "index" = "index_1" THEN 1 ELSE 0 END'

    sql = f'''
        SELECT 
            "index",
            "index_1",
            cpu_time_total,
            cpu_time_self,
            cpu_time_children,
            COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0) as cpu_sum,
            name,
            {parent_select},
            {is_parent_sql} as is_parent
        FROM "{TABLE_NAME}"
        {where_sql}
        {order_sql}
        LIMIT ?
    '''
    params.append(limit)
    
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    
    # If we're in index view (current_index set), try to find the parent's parent (caller) row:
    caller_row = None
    if current_index:
        try:
            caller_params = [int(current_index)]
            caller_sql = f'''
                SELECT 
                    "index",
                    "index_1",
                    cpu_time_total,
                    cpu_time_self,
                    cpu_time_children,
                    COALESCE(cpu_time_self, 0) + COALESCE(cpu_time_children, 0) as cpu_sum,
                    name,
                    {parent_select},
                    {is_parent_sql} as is_parent
                FROM "{TABLE_NAME}"
                WHERE "index_1" = ? AND "index" != "index_1"
                ORDER BY cpu_time_total DESC
                LIMIT 1
            '''
            cursor = conn.cursor()
            cursor.execute(caller_sql, caller_params)
            caller_row = cursor.fetchone()
        except Exception:
            caller_row = None

    conn.close()
    
    # Build table HTML
    if rows:
        table_rows = ""

        # Render caller (parent's parent) row first, if available
        if caller_row is not None:
            # The caller row represents the parent's parent. Display its own index (first column) as the index link.
            caller_index_display = caller_row['index']
            caller_index_link = make_link({'index': caller_index_display, 'view': 'children', 'db': db_param}) if caller_index_display else None
            caller_index_cell = f'<a href="{caller_index_link}" class="index-link">[{caller_index_display}]</a>' if caller_index_link else ""

            caller_parent_cell = ""
            if caller_row['parent_index']:
                caller_parent_link = make_link({'index': caller_row['parent_index'], 'view': 'children', 'db': db_param})
                caller_parent_cell = f'<a href="{caller_parent_link}" class="index-link">[{caller_row["parent_index"]}]</a>'

            caller_cpu_total = f"{caller_row['cpu_time_total']:.2f}" if caller_row['cpu_time_total'] is not None else ""
            caller_cpu_self = f"{caller_row['cpu_time_self']:.6f}" if caller_row['cpu_time_self'] is not None else ""
            caller_cpu_children = f"{caller_row['cpu_time_children']:.6f}" if caller_row['cpu_time_children'] is not None else ""
            caller_cpu_sum = f"{caller_row['cpu_sum']:.6f}" if caller_row['cpu_sum'] is not None else ""

            # Fetch the caller's own function name from its parent row so we show the caller's name, not the callee's
            caller_name_value = None
            try:
                # Determine the appropriate predicate to identify a parent row for the caller function
                if has_parent_index:
                    name_sql = f'SELECT name FROM "{TABLE_NAME}" WHERE "index" = ? AND parent_index IS NULL LIMIT 1'
                else:
                    name_sql = f'SELECT name FROM "{TABLE_NAME}" WHERE "index" = ? AND "index" = "index_1" LIMIT 1'
                # Re-open a connection just for this quick lookup (previous conn is closed by now)
                with sqlite3.connect(db_path) as name_conn:
                    name_conn.row_factory = sqlite3.Row
                    name_cur = name_conn.cursor()
                    name_cur.execute(name_sql, (caller_index_display,))
                    name_row = name_cur.fetchone()
                    if name_row:
                        caller_name_value = name_row['name']
            except Exception:
                caller_name_value = None

            caller_name_html = format_name_with_links(caller_name_value if caller_name_value is not None else caller_row['name'], db_param)

            table_rows += f'''
            <tr class="caller-row" onclick="window.location.href='{caller_index_link}';" title="Open caller index [{caller_index_display}] view">
                <td>{caller_index_cell}</td>
                <td class="right-align">{caller_cpu_total}</td>
                <td class="right-align">{caller_cpu_self}</td>
                <td class="right-align">{caller_cpu_children}</td>
                <td class="right-align">{caller_cpu_sum}</td>
                <td>{caller_name_html}</td>
                <td>{caller_parent_cell}</td>
            </tr>
            '''

        for row in rows:
            row_class = "parent-row" if row['is_parent'] else ("child-row" if row['parent_index'] else "")

            # Display logic:
            # - For parent rows show the parent's own index (first column)
            # - For child rows show the child's own index (last column)
            display_index = row['index'] if row['is_parent'] else row['index_1']

            index_cell = ""
            index_link = None
            if display_index:
                index_link = make_link({'index': display_index, 'view': 'children', 'db': db_param})
                index_cell = f'<a href="{index_link}" class="index-link">[{display_index}]</a>'

            parent_cell = ""
            if row['parent_index']:
                parent_link = make_link({'index': row['parent_index'], 'view': 'children', 'db': db_param})
                parent_cell = f'<a href="{parent_link}" class="index-link">[{row["parent_index"]}]</a>'

            cpu_total = f"{row['cpu_time_total']:.2f}" if row['cpu_time_total'] is not None else ""
            cpu_self = f"{row['cpu_time_self']:.6f}" if row['cpu_time_self'] is not None else ""
            cpu_children = f"{row['cpu_time_children']:.6f}" if row['cpu_time_children'] is not None else ""
            cpu_sum = f"{row['cpu_sum']:.6f}" if row['cpu_sum'] is not None else ""

            # Format name with clickable links for trailing indices
            name_html = format_name_with_links(row['name'], db_param)

            # Make entire parent row clickable to open index view
            row_attrs = f'class="{row_class}"'
            if row['is_parent'] and index_link:
                # Use onclick navigation and title for affordance
                row_attrs += f' onclick="window.location.href=\'{index_link}\';" title="Open index [{display_index}] view"'

            # In the parent+children view, make child rows clickable to open that child's own index view
            if (not row['is_parent']) and index_link and current_index:
                row_attrs += f' onclick="window.location.href=\'{index_link}\';" title="Open child index [{display_index}] view" style="cursor: pointer;"'

            table_rows += f'''
            <tr {row_attrs}>
                <td>{index_cell}</td>
                <td class="right-align">{cpu_total}</td>
                <td class="right-align">{cpu_self}</td>
                <td class="right-align">{cpu_children}</td>
                <td class="right-align">{cpu_sum}</td>
                <td>{name_html}</td>
                <td>{parent_cell}</td>
            </tr>
            '''
        
        table_content = f'''
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
                {table_rows}
            </tbody>
        </table>
        '''
    else:
        table_content = '<div class="no-data">No data found.</div>'
    
    # Build title section
    title_section = f"<h2>{html_escape(title)}</h2>" if title else ""
    
    # Hidden field for database selection in controls form
    db_field = f'<input type="hidden" name="db" value="{html_escape(db_param)}">'
    
    # Print HTML
    print(HTML_TEMPLATE.format(
        db_selector=db_selector,
        breadcrumb=breadcrumb,
        db_field=db_field,
        selected_all='selected' if view == 'all' else '',
        selected_parents='selected' if view == 'parents' else '',
        selected_top_cpu='selected' if view == 'top_cpu' else '',
        limit=limit,
        search=html_escape(search),
        current_index=current_index,
        db_name=html_escape(db_name),
        total_rows=total_rows,
        showing_rows=(len(rows) + (1 if (current_index and caller_row is not None) else 0)),
        title_section=title_section,
        table_content=table_content
    ))


if __name__ == "__main__":
    main()
