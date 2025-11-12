from app_local import app
from backend.connect_local import connect_local, initialize_database


def setup():
    db = connect_local()
    cursor, cnxn = db.connect_to_db()
    initialize_database(cursor, cnxn, initial_values=False)
    # Seed minimal data
    cursor.execute("INSERT INTO POs (name) VALUES (?)", ("PO1",))
    po_id = cursor.lastrowid
    cursor.execute("INSERT INTO departments (name, po_id) VALUES (?, ?)", ("Dept1", po_id))
    dept_id = cursor.lastrowid
    cursor.execute("INSERT INTO human_resource_categories (name) VALUES (?)", ("Engineer",))
    cat_id = cursor.lastrowid
    cursor.execute(
        "INSERT INTO human_resource_cost (category_id, year, po_id, department_id, cost) VALUES (?, ?, ?, ?, ?)",
        (cat_id, 2025, po_id, dept_id, 123.45),
    )
    cnxn.commit()


def test_endpoint():
    setup()
    client = app.test_client()
    resp = client.get("/api/hr_cost?category=Engineer&year=2025&po=PO1&department=Dept1")
    assert resp.status_code == 200, resp.status_code
    data = resp.get_json()
    assert data and round(float(data.get("cost")), 2) == 123.45, data
    print("PASS: /api/hr_cost hierarchical lookup returns expected cost")


if __name__ == "__main__":
    test_endpoint()
