import json
import firebase_admin
from firebase_admin import firestore
from firebase_admin import auth as firebase_auth
from firebase_functions import https_fn, options
from datetime import datetime
from functools import wraps

# Inicializar Firebase Admin
firebase_admin.initialize_app()
db = firestore.client()

# Constantes
CORS_OPTIONS = options.CorsOptions(
    cors_origins="*", 
    cors_methods=["get", "post", "put", "delete", "options"]
)

# Decoradores y Helpers
def authenticated_handler(f):
    @wraps(f)
    def wrapper(req: https_fn.CallableRequest):
        if not req.auth:
            return {"error": "User must be authenticated"}
        
        uid = req.auth.uid
        return f(req, uid)
    return wrapper

def table_ref(user_id: str, table_id: str):
    return db.collection("users").document(user_id).collection("tables").document(table_id)

def get_table_doc(user_id: str, table_id: str):
    if not table_id:
        raise ValueError("tableId es requerido")
    doc_ref = table_ref(user_id, table_id)
    doc = doc_ref.get()
    if not doc.exists:
        raise ValueError("Table not found")
    return doc_ref, doc

def map_row_values(columns, values):
    """Convierte lista de valores en objeto con keys de columnas"""
    mapped = {}
    for i, col in enumerate(columns):
        key = col["Key"]
        val = values[i] if i < len(values) else None
        # Conversión de booleanos desde string
        if isinstance(val, str) and val.strip().lower() in ("true", "false"):
            val = val.strip().lower() == "true"
        mapped[key] = val
    return mapped


# Adaptador para solicitudes HTTP (Flask) -> formato esperado por los handlers on_call
class RequestAdapter:
    def __init__(self, flask_req):
        self._flask = flask_req
        # Intentar parsear JSON del body, caer a diccionario vacío
        try:
            self.data = flask_req.get_json(silent=True) or {}
        except Exception:
            self.data = {}
        # Exponer headers y args de forma compatible
        self.headers = flask_req.headers
        self.args = flask_req.args


def _make_response(obj):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Content-Type": "application/json"
    }
    return https_fn.Response(json.dumps(obj), headers=headers)

# -----------------------------
# TABLAS
# -----------------------------
@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def create_table(req: https_fn.CallableRequest, user_id: str):
    data = req.data
    name = data.get("Name")
    columns = data.get("Columns", [])
    rows = data.get("Rows", [])

    if not name or not columns:
        return {"error": "Name y Columns son requeridos"}

    table_doc = db.collection("users").document(user_id).collection("tables").document()
    mapped_rows = [map_row_values(columns, row.get("Values", [])) for row in rows]

    table_doc.set({
        "Name": name,
        "Columns": columns,
        "Rows": mapped_rows,
        "Metadata": data.get("Metadata", {}),
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
    })

    return {"tableId": table_doc.id}


# HTTP adapter
@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def create_table_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = create_table(adapted, user_id)
    return _make_response(result)

@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def list_tables(req: https_fn.CallableRequest, user_id: str):
    snapshot = db.collection("users").document(user_id).collection("tables").get()
    tables = [{"tableId": doc.id, **doc.to_dict()} for doc in snapshot]
    
    return {"tables": tables}  # Solo devuelve un dict, no uses Response ni headers

@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def update_table(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    updates = req.data.get("updates", {})

    if not table_id:
        return {"error": "tableId es requerido"}

    table_ref(user_id, table_id).update(updates)
    return {"success": True}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def update_table_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = update_table(adapted, user_id)
    return _make_response(result)

@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def delete_table(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    if not table_id:
        return {"error": "tableId es requerido"}

    table_ref(user_id, table_id).delete()
    return {"success": True}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def delete_table_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = delete_table(adapted, user_id)
    return _make_response(result)

@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def clone_table(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    
    try:
        _, orig_table = get_table_doc(user_id, table_id)
    except ValueError as e:
        return {"error": str(e)}

    data = orig_table.to_dict()
    new_table = db.collection("users").document(user_id).collection("tables").document()
    new_table.set(data)
    return {"tableId": new_table.id}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def clone_table_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = clone_table(adapted, user_id)
    return _make_response(result)

# -----------------------------
# COLUMNAS
# -----------------------------
@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def add_column(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    column = req.data.get("column")

    if not column:
        return {"error": "column es requerido"}

    try:
        doc_ref, doc = get_table_doc(user_id, table_id)
    except ValueError as e:
        return {"error": str(e)}

    columns = doc.to_dict().get("Columns", [])
    columns.append(column)
    doc_ref.update({"Columns": columns})
    return {"success": True}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def add_column_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = add_column(adapted, user_id)
    return _make_response(result)

@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def update_column(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    key = req.data.get("columnKey")
    updates = req.data.get("updates", {})

    if not key:
        return {"error": "columnKey es requerido"}

    try:
        doc_ref, doc = get_table_doc(user_id, table_id)
    except ValueError as e:
        return {"error": str(e)}

    columns = doc.to_dict().get("Columns", [])
    for col in columns:
        if col["Key"] == key:
            col.update(updates)
            break
    doc_ref.update({"Columns": columns})
    return {"success": True}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def update_column_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = update_column(adapted, user_id)
    return _make_response(result)

@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def delete_column(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    key = req.data.get("columnKey")

    if not key:
        return {"error": "columnKey es requerido"}

    try:
        doc_ref, doc = get_table_doc(user_id, table_id)
    except ValueError as e:
        return {"error": str(e)}

    columns = doc.to_dict().get("Columns", [])
    columns = [col for col in columns if col["Key"] != key]

    rows = doc.to_dict().get("Rows", [])
    for row in rows:
        row.pop(key, None)  # pop con default para evitar KeyError

    doc_ref.update({"Columns": columns, "Rows": rows})
    return {"success": True}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def delete_column_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = delete_column(adapted, user_id)
    return _make_response(result)

# -----------------------------
# FILAS
# -----------------------------
@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def add_row(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    row = req.data.get("row", {})

    if not row:
        return {"error": "row son requeridos"}

    try:
        doc_ref, doc = get_table_doc(user_id, table_id)
    except ValueError as e:
        return {"error": str(e)}

    rows = doc.to_dict().get("Rows", [])
    rows.append(row)
    doc_ref.update({"Rows": rows})
    return {"success": True}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def add_row_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = add_row(adapted, user_id)
    return _make_response(result)

@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def update_row(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    index = req.data.get("rowIndex")
    updates = req.data.get("updates", {})

    if index is None:
        return {"error": "rowIndex son requeridos"}

    try:
        doc_ref, doc = get_table_doc(user_id, table_id)
    except ValueError as e:
        return {"error": str(e)}

    rows = doc.to_dict().get("Rows", [])
    if index < 0 or index >= len(rows):
        return {"error": "Row index out of range"}

    rows[index].update(updates)
    doc_ref.update({"Rows": rows})
    return {"success": True}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def update_row_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = update_row(adapted, user_id)
    return _make_response(result)

@https_fn.on_call(cors=CORS_OPTIONS)
@authenticated_handler
def delete_row(req: https_fn.CallableRequest, user_id: str):
    table_id = req.data.get("tableId")
    index = req.data.get("rowIndex")

    if index is None:
        return {"error": "rowIndex son requeridos"}

    try:
        doc_ref, doc = get_table_doc(user_id, table_id)
    except ValueError as e:
        return {"error": str(e)}

    rows = doc.to_dict().get("Rows", [])
    if index < 0 or index >= len(rows):
        return {"error": "Row index out of range"}

    rows.pop(index)
    doc_ref.update({"Rows": rows})
    return {"success": True}


@https_fn.on_request(cors=CORS_OPTIONS)
@authenticated_handler
def delete_row_http(req, user_id: str):
    adapted = RequestAdapter(req)
    result = delete_row(adapted, user_id)
    return _make_response(result)