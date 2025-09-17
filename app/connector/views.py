from ninja import Router, Body
import duckdb, hashlib, time, json, yaml, logging, os
from pathlib import Path
from .utils import build_sql
from ninja_jwt.authentication import JWTAuth
import re


router = Router()
logger = logging.getLogger(__name__)


# (NEW) если есть Django settings — используем BASE_DIR для абсолютных путей
try:
    from django.conf import settings
    BASE_DIR = Path(settings.BASE_DIR)
except Exception:
    BASE_DIR = Path(__file__).resolve().parents[2]  # fallback


# --- Конфиг (ABS paths + существование каталога) ---
CFG_PATH = (BASE_DIR / "connector/snapshots/test-connector/mapping.yml").resolve()
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

STORAGE_ROOT = (BASE_DIR / CFG["storage"]["root"]).resolve()
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

MANIFEST_FILE = STORAGE_ROOT / CFG["storage"].get("manifest", "manifest.json")

_DB = None
_snapshot_initialized = False  # флаг в пределах процесса


# проверка подписи

# manifest = {}
# if MANIFEST_FILE.exists():
#     try:
#         manifest = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
#     except Exception as e:
#         logger.warning(f"Failed to read manifest.json: {e}")


def get_db():
    """Ленивое подключение к DuckDB (всегда файл, не :memory:)"""
    global _DB
    if _DB is None:
        _DB = duckdb.connect()
        logger.info(f"Connected to DuckDB")

    return _DB


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
    con = get_db()

    for group_name in requested_groups:
        group_cfg = (CFG.get("groups") or {}).get(group_name)   # выбираем нужную группу из "groups" по названию "group_name"
        schemas = CFG.get("schemas")
        if not group_cfg:
            logger.info(f'Такой группы в mapping.yml нет: {group_name}')
            continue

        sql = build_sql(group_cfg, subject)

        schema = group_cfg['from']
        schema_name = schema['schema']
        join_schema = schema.get('join')

        # schema_name
        schema_cfg = schemas.get(schema_name)
        parquet_file = schema_cfg.get('path')
        parquet_path = (STORAGE_ROOT / parquet_file).resolve()

        sql = sql.replace(f"FROM {schema_name}", f"FROM read_parquet('{parquet_path}') AS {schema_name}")
        # если есть JOIN в группе group_cfg
        if join_schema:
            join_schema_name = join_schema[0].get("schema")
            schema_cfg = schemas.get(join_schema_name)
            parquet_file = schema_cfg.get("path")
            parquet_path = (STORAGE_ROOT / parquet_file).resolve()
            sql = sql.replace(f"JOIN {join_schema_name}", f"JOIN read_parquet('{parquet_path}') AS {join_schema_name}")

        data[group_name] = con.execute(sql).fetch_arrow_table().to_pylist()


    latency = int((time.time() - start) * 1000)
    return {
        "source_id": source_id,
        "status": "ok",
        "source_status": "live",
        "latency_ms": latency,
        "data": data
    }
