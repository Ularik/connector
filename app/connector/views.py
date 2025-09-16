from ninja import Router, Body
import duckdb, hashlib, time, json, yaml
from pathlib import Path
from .utils import build_sql
from ninja_jwt.authentication import JWTAuth
import logging

logger = logging.getLogger(__name__)
router = Router()

# --- Конфиг ---
CFG_PATH = Path("connector/snapshots/test-connector/mapping.yml")
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

STORAGE_ROOT = Path(CFG["storage"]["root"])
MANIFEST_FILE = STORAGE_ROOT / CFG["storage"].get("manifest", "")

# глобальная ссылка на соединение
_DB = None


def get_db():
    """Ленивое подключение к DuckDB"""
    global _DB
    if _DB is None:
        db_path = STORAGE_ROOT / "_cache.duckdb"
        _DB = duckdb.connect(db_path)
        logger.warning("📋 Tables in DB:", _DB.execute("SHOW TABLES").fetchall())
        print("car.parquet exists:", (STORAGE_ROOT / "car.parquet").exists())
    return _DB


def init_snapshot():
    """Инициализация снепшота — создаёт таблицы в DuckDB"""
    manifest = {}
    if MANIFEST_FILE.exists():
        manifest = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))

    db = get_db()

    for schema_name, schema_cfg in CFG.get("schemas", {}).items():
        file_path = STORAGE_ROOT / schema_cfg["path"]

        # проверка хэша (можно раскомментировать при необходимости)
        # h = manifest.get("hashes", {}).get(schema_cfg["path"])
        # if h and h.startswith("sha256:"):
        #     exp = h.split(":", 1)[1]
        #     real = hashlib.sha256(file_path.read_bytes()).hexdigest()
        #     if real != exp:
        #         raise RuntimeError(f"Hash mismatch for {file_path.name}")

        # создаём таблицу в DuckDB
        db.execute(f"DROP TABLE IF EXISTS {schema_name}")
        if file_path.suffix.lower() == ".parquet":
            db.execute(f"CREATE TABLE {schema_name} AS SELECT * FROM read_parquet('{str(file_path)}')")
        else:
            db.execute(f"CREATE TABLE {schema_name} AS SELECT * FROM read_csv_auto('{str(file_path)}', header=true)")


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
