from ninja import Router, Body
import duckdb, hashlib, time, json, yaml, logging, os
from pathlib import Path
from .utils import build_sql
from ninja_jwt.authentication import JWTAuth
import re

def q_ident(name: str) -> str:
    """
    Безопасно экранирует имя идентификатора для SQL (таблица/схема/поле).
    Разрешаем [A-Za-z_][A-Za-z0-9_]* без кавычек; иначе — берем в двойные кавычки и удваиваем внутренние.
    """
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        return name
    return '"' + name.replace('"', '""') + '"'


# (NEW) если есть Django settings — используем BASE_DIR для абсолютных путей
try:
    from django.conf import settings
    BASE_DIR = Path(settings.BASE_DIR)
except Exception:
    BASE_DIR = Path(__file__).resolve().parents[2]  # fallback

router = Router()
logger = logging.getLogger(__name__)

# --- Конфиг (ABS paths + существование каталога) ---
CFG_PATH = (BASE_DIR / "connector/snapshots/test-connector/mapping.yml").resolve()
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

STORAGE_ROOT = (BASE_DIR / CFG["storage"]["root"]).resolve()
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

MANIFEST_FILE = STORAGE_ROOT / CFG["storage"].get("manifest", "manifest.json")

_DB = None
_snapshot_initialized = False  # флаг в пределах процесса

def get_db():
    """Ленивое подключение к DuckDB (всегда файл, не :memory:)"""
    global _DB
    if _DB is None:
        db_path = (STORAGE_ROOT / "_cache.duckdb").resolve()
        _DB = duckdb.connect(str(db_path))
        logger.info(f"Connected to DuckDB at {db_path}")
        logger.info("PRAGMA database_list -> %s",
                    _DB.execute("PRAGMA database_list").fetchall())
        logger.info("Tables in DB (on connect): %s",
                    _DB.execute("SHOW TABLES").fetchall())
    return _DB

def init_snapshot():
    """Полная (пере)инициализация снепшота — создаёт/обновляет таблицы в DuckDB"""
    global _snapshot_initialized
    db = get_db()

    manifest = {}
    if MANIFEST_FILE.exists():
        try:
            manifest = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"Failed to read manifest.json: {e}")

    # создаём таблицы заново (idempotent: DROP IF EXISTS)
    for schema_name, schema_cfg in CFG.get("schemas", {}).items():
        file_path = (STORAGE_ROOT / schema_cfg["path"]).resolve()

        if not file_path.exists():
            logger.warning(f"File does not exist: {file_path}")
            # таблица для такого schema_name создана не будет
            continue

        db.execute(f"DROP TABLE IF EXISTS {q_ident(schema_name)}")
        if file_path.suffix.lower() == ".parquet":
            db.execute(
                f"CREATE TABLE {q_ident(schema_name)} "
                f"AS SELECT * FROM read_parquet('{file_path.as_posix()}')"
            )
        else:
            db.execute(
                f"CREATE TABLE {q_ident(schema_name)} "
                f"AS SELECT * FROM read_csv_auto('{file_path.as_posix()}', header=true)"
            )

    tables = db.execute("SHOW TABLES").fetchall()
    logger.info(f"Snapshot initialized. Tables: {tables}")
    _snapshot_initialized = True

def ensure_snapshot():
    """Гарантирует, что все таблицы из CFG['schemas'] существуют; иначе — инициализирует."""
    db = get_db()
    existing = {name for (name,) in db.execute("SHOW TABLES").fetchall()}
    expected = set((CFG.get("schemas") or {}).keys())

    # если хотя бы одной ожидаемой таблицы нет — инициализация
    if not expected.issubset(existing):
        logger.info("Missing tables %s -> running init_snapshot()",
                    sorted(expected - existing))
        init_snapshot()

# --- API endpoints ---
@router.get('/get-info', response={200: str, 400: str})
def get_list(request):
    return 200, 'good connect'

@router.post("/v1/lookup", auth=JWTAuth())
def lookup(request, payload: dict = Body(...)):
    ensure_snapshot()  # (NEW) гарантируем наличие таблиц перед первым использованием

    source_id = payload.get("source_id", "CARS")
    subject = payload.get("subject", {})
    requested_groups = payload.get("requested_fields", []) or []

    start = time.time()
    data = {}
    db = get_db()

    for group_name in requested_groups:
        group_cfg = (CFG.get("groups") or {}).get(group_name)
        if not group_cfg:
            continue

        sql = build_sql(group_cfg, subject)
        logger.info("Executing SQL for group '%s': %s", group_name, sql)

        # (NEW) один безопасный ретрай, если таблица внезапно не найдена
        try:
            data[group_name] = db.execute(sql).fetch_arrow_table().to_pylist()
        except duckdb.CatalogException as e:
            logger.warning("CatalogException on first try (%s). Re-init snapshot and retry once.", e)
            init_snapshot()
            data[group_name] = db.execute(sql).fetch_arrow_table().to_pylist()

    latency = int((time.time() - start) * 1000)
    return {
        "source_id": source_id,
        "status": "ok",
        "source_status": "live",
        "latency_ms": latency,
        "data": data
    }
