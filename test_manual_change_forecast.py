"""Integration test for manual_change_forecast forward (FTE->cost) and reverse (Cost->FTE) logic.

This script initializes an empty local SQLite database, inserts minimal reference
data plus one personnel forecast row, then exercises the /manual_input/change_forecast
endpoint twice:

1. Reverse mode: provide cost__<slug> to derive FTE from hierarchical unit cost.
2. Forward mode: provide fte__<slug> to directly set new FTE value.

Expected outcomes:
 - After reverse update with cost 1500 and unit rate 100, FTE becomes 15.
 - After forward update with FTE 20, FTE becomes 20.

Run: python test_manual_change_forecast.py
It will print PASS or raise AssertionError on failure.
"""

from app_local import app  # imports and registers blueprints
from backend.connect_local import connect_local, initialize_database
import sqlite3


def setup_db():
    db = connect_local()
    cursor, cnxn = db.connect_to_db()
    initialize_database(cursor, cnxn, initial_values=False)

    # Insert PO
    cursor.execute("INSERT INTO POs (name) VALUES (?)", ("PO1",))
    po_id = cursor.lastrowid
    # Insert Department linked to PO
    cursor.execute("INSERT INTO departments (name, po_id) VALUES (?, ?)", ("Dept1", po_id))
    dept_id = cursor.lastrowid
    # Insert Project Category
    cursor.execute("INSERT INTO project_categories (category) VALUES (?)", ("CatA",))
    pc_id = cursor.lastrowid
    # Insert Project
    cursor.execute(
        "INSERT INTO projects (name, category_id, department_id, fiscal_year) VALUES (?, ?, ?, ?)",
        ("Proj1", pc_id, dept_id, 2025),
    )
    proj_id = cursor.lastrowid
    # Insert IO
    cursor.execute("INSERT INTO IOs (IO_num, project_id) VALUES (?, ?)", (100, proj_id))
    io_id = cursor.lastrowid
    # Insert HR Category
    cursor.execute("INSERT INTO human_resource_categories (name) VALUES (?)", ("Engineer",))
    hr_cat_id = cursor.lastrowid
    # Insert hierarchical unit cost (most specific: po+dept+cat+year) cost=100
    cursor.execute(
        "INSERT INTO human_resource_cost (category_id, year, po_id, department_id, cost) VALUES (?, ?, ?, ?, ?)",
        (hr_cat_id, 2025, po_id, dept_id, 100.0),
    )
    # Insert personnel forecast row with initial FTE=10
    cursor.execute(
        """
        INSERT INTO project_forecasts_pc (
            PO_id, department_id, project_category_id, project_id, io_id, fiscal_year,
            human_resource_category_id, human_resource_fte
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (po_id, dept_id, pc_id, proj_id, io_id, 2025, hr_cat_id, 10.0),
    )
    cnxn.commit()
    return db, cursor, cnxn, {
        "po_id": po_id,
        "dept_id": dept_id,
        "pc_id": pc_id,
        "proj_id": proj_id,
        "io_id": io_id,
        "hr_cat_id": hr_cat_id,
    }


def get_current_fte(cursor):
    cursor.execute("SELECT human_resource_fte FROM project_forecasts_pc")
    rows = cursor.fetchall()
    return rows[0][0] if rows else None


def run_tests():
    db, cursor, cnxn, ids = setup_db()

    client = app.test_client()

    # Reverse mode: send cost__slug1=1500; cat__slug1=Engineer
    reverse_payload = {
        "PO": "PO1",
        "Department": "Dept1",
        "Project_Name": "Proj1",
        "Project_Category": "CatA",
        "fiscal_year": "2025",
        "IO": "100",
        "cat__slug1": "Engineer",
        "cost__slug1": "1500"  # expect FTE 15 (1500 / 100)
    }
    resp = client.post("/manual_input/change_forecast", data=reverse_payload, follow_redirects=True)
    assert resp.status_code == 200, f"Reverse update HTTP {resp.status_code}"
    fte_after_reverse = get_current_fte(cursor)
    assert round(fte_after_reverse, 4) == 15.0, f"Reverse mode FTE expected 15.0 got {fte_after_reverse}"

    # Forward mode: send fte__slug1=20; cat__slug1=Engineer
    forward_payload = {
        "PO": "PO1",
        "Department": "Dept1",
        "Project_Name": "Proj1",
        "Project_Category": "CatA",
        "fiscal_year": "2025",
        "IO": "100",
        "cat__slug1": "Engineer",
        "fte__slug1": "20"  # expect FTE updated directly to 20
    }
    resp2 = client.post("/manual_input/change_forecast", data=forward_payload, follow_redirects=True)
    assert resp2.status_code == 200, f"Forward update HTTP {resp2.status_code}"
    fte_after_forward = get_current_fte(cursor)
    assert round(fte_after_forward, 4) == 20.0, f"Forward mode FTE expected 20.0 got {fte_after_forward}"

    print("PASS: manual_change_forecast reverse and forward updates behaved as expected.")


if __name__ == "__main__":
    run_tests()
