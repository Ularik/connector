from ninja import Router, Body
import duckdb, hashlib, time, json, yaml, logging
from pathlib import Path
from .utils import build_sql
from ninja_jwt.authentication import JWTAuth

router = Router()
logger = logging.getLogger(__name__)

# --- Конфиг ---
CFG_PATH = Path("connector/snapshots/test-connector/mapping.yml")
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

STORAGE_ROOT = Path(CFG["storage"]["root"])
MANIFEST_FILE = STORAGE_ROOT / CFG["storage"].get("manifest", "")

# глобальная ссылка на соединение
_DB = None
_snapshot_initialized = False  # флаг, чтобы не инициализировать повторно

def get_db():
    """Ленивое подключение к DuckDB"""
    global _DB
    if _DB is None:
        db_path = STORAGE_ROOT / "_cache.duckdb"
        _DB = duckdb.connect(str(db_path))
        logger.info(f"Connected to DuckDB at {db_path}")
        # Проверим существующие таблицы
        tables = _DB.execute("SHOW TABLES").fetchall()
        logger.info(f"Tables in DB: {tables}")
    return _DB

def init_snapshot():
    """Инициализация снепшота — создаёт таблицы в DuckDB"""
    global _snapshot_initialized
    if _snapshot_initialized:
        return  # уже инициализировано

    db = get_db()
    manifest = {}
    if MANIFEST_FILE.exists():
        try:
            manifest = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"Failed to read manifest.json: {e}")

    for schema_name, schema_cfg in CFG.get("schemas", {}).items():
        file_path = STORAGE_ROOT / schema_cfg["path"]

        if not file_path.exists():
            logger.warning(f"File does not exist: {file_path}")
            continue

        # создаём таблицу в DuckDB
        db.execute(f"DROP TABLE IF EXISTS {schema_name}")
        if file_path.suffix.lower() == ".parquet":
            db.execute(f"CREATE TABLE {schema_name} AS SELECT * FROM read_parquet('{str(file_path)}')")
        else:
            db.execute(f"CREATE TABLE {schema_name} AS SELECT * FROM read_csv_auto('{str(file_path)}', header=true)")

    tables = db.execute("SHOW TABLES").fetchall()
    logger.info(f"Snapshot initialized. Tables: {tables}")
    _snapshot_initialized = True


# --- API endpoints ---
@router.get('/get-info', response={200: str, 400: str})
def get_list(request):
    return 200, 'good connect'


@router.post("/v1/lookup", auth=JWTAuth())
def lookup(request, payload: dict = Body(...)):

    source_id = payload.get("source_id", "CARS")
    subject = payload.get("subject", {})
    requested_groups = payload.get("requested_fields", []) or []

    start = time.time()
    data = {}
    db = get_db()

    for group_name in requested_groups:
        group_cfg = CFG["groups"].get(group_name)
        if not group_cfg:
            continue
        sql = build_sql(group_cfg, subject)

        print(sql)
        data[group_name] = db.execute(sql).fetch_arrow_table().to_pylist()

    latency = int((time.time() - start) * 1000)
    return {
        "source_id": source_id,
        "status": "ok",
        "source_status": "live",
        "latency_ms": latency,
        "data": data
    }
