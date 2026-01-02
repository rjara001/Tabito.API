import firebase_admin
from firebase_admin import firestore
from firebase_functions import https_fn

# -----------------------------
# Inicializar Firebase Admin
# -----------------------------
firebase_admin.initialize_app()
db = firestore.client()

# -----------------------------
# Helpers
# -----------------------------
def table_ref(user_id: str, table_id: str):
    return db.collection("users").document(user_id).collection("tables").document(table_id)

def row_ref(user_id: str, table_id: str, row_id: str):
    return table_ref(user_id, table_id).collection("rows").document(row_id)

def map_row_values(columns, values):
    """Convierte lista de valores en objeto con keys de columnas"""
    mapped = {}
    for i, col in enumerate(columns):
        key = col["Key"]
        val = values[i] if i < len(values) else None
        # Conversión de booleanos
        if isinstance(val, str):
            if val.lower() == "true":
                val = True
            elif val.lower() == "false":
                val = False
        mapped[key] = val
    return mapped

# -----------------------------
# TABLAS
# -----------------------------
@https_fn.on_call()
def create_table(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    data = req.data
    name = data.get("Name")
    columns = data.get("Columns", [])
    rows = data.get("Rows", [])

    table_doc = db.collection("users").document(user_id).collection("tables").document()
    # Map rows
    mapped_rows = [map_row_values(columns, row.get("Values", [])) for row in rows]
    
    table_doc.set({
        "Name": name,
        "Columns": columns,
        "Rows": mapped_rows,
        "Metadata": data.get("Metadata", {}),
    })
    return {"tableId": table_doc.id}

@https_fn.on_call()
def list_tables(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}
    
    user_id = context.auth.uid
    snapshot = db.collection("users").document(user_id).collection("tables").get()
    tables = []
    for doc in snapshot:
        tables.append({"tableId": doc.id, **doc.to_dict()})
    return {"tables": tables}

@https_fn.on_call()
def update_table(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    table_id = req.data.get("tableId")
    updates = req.data.get("updates", {})

    table_ref(user_id, table_id).update(updates)
    return {"success": True}

@https_fn.on_call()
def delete_table(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    table_id = req.data.get("tableId")

    table_ref(user_id, table_id).delete()
    return {"success": True}

@https_fn.on_call()
def clone_table(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    table_id = req.data.get("tableId")
    orig_table = table_ref(user_id, table_id).get()
    if not orig_table.exists:
        return {"error": "Table not found"}

    data = orig_table.to_dict()
    new_table = db.collection("users").document(user_id).collection("tables").document()
    new_table.set(data)
    return {"tableId": new_table.id}

# -----------------------------
# COLUMNAS
# -----------------------------
@https_fn.on_call()
def add_column(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}
    
    user_id = context.auth.uid
    table_id = req.data.get("tableId")
    column = req.data.get("column")

    doc_ref = table_ref(user_id, table_id)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": "Table not found"}

    columns = doc.to_dict().get("Columns", [])
    columns.append(column)
    doc_ref.update({"Columns": columns})
    return {"success": True}

@https_fn.on_call()
def update_column(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    table_id = req.data.get("tableId")
    key = req.data.get("columnKey")
    updates = req.data.get("updates", {})

    doc_ref = table_ref(user_id, table_id)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": "Table not found"}

    columns = doc.to_dict().get("Columns", [])
    for col in columns:
        if col["Key"] == key:
            col.update(updates)
            break
    doc_ref.update({"Columns": columns})
    return {"success": True}

@https_fn.on_call()
def delete_column(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    table_id = req.data.get("tableId")
    key = req.data.get("columnKey")

    doc_ref = table_ref(user_id, table_id)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": "Table not found"}

    columns = doc.to_dict().get("Columns", [])
    columns = [col for col in columns if col["Key"] != key]

    # Opcional: actualizar filas para eliminar este campo
    rows = doc.to_dict().get("Rows", [])
    for row in rows:
        if key in row:
            del row[key]

    doc_ref.update({"Columns": columns, "Rows": rows})
    return {"success": True}

# -----------------------------
# FILAS
# -----------------------------
@https_fn.on_call()
def add_row(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    table_id = req.data.get("tableId")
    row = req.data.get("row", {})

    doc_ref = table_ref(user_id, table_id)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": "Table not found"}

    rows = doc.to_dict().get("Rows", [])
    rows.append(row)
    doc_ref.update({"Rows": rows})
    return {"success": True}

@https_fn.on_call()
def update_row(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    table_id = req.data.get("tableId")
    index = req.data.get("rowIndex")
    updates = req.data.get("updates", {})

    doc_ref = table_ref(user_id, table_id)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": "Table not found"}

    rows = doc.to_dict().get("Rows", [])
    if index < 0 or index >= len(rows):
        return {"error": "Row index out of range"}

    rows[index].update(updates)
    doc_ref.update({"Rows": rows})
    return {"success": True}

@https_fn.on_call()
def delete_row(req, context):
    if not context.auth:
        return {"error": "User must be authenticated"}

    user_id = context.auth.uid
    table_id = req.data.get("tableId")
    index = req.data.get("rowIndex")

    doc_ref = table_ref(user_id, table_id)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": "Table not found"}

    rows = doc.to_dict().get("Rows", [])
    if index < 0 or index >= len(rows):
        return {"error": "Row index out of range"}

    rows.pop(index)
    doc_ref.update({"Rows": rows})
    return {"success": True}
