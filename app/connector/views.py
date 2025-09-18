from ninja import Router, Body
import duckdb, hashlib, time, json, yaml, logging
from pathlib import Path
from .utils import build_sql
from ninja_jwt.authentication import JWTAuth
from jose import jwt
import os
from project.settings_local import SECRETS_PATH


# для jws токена
PRIVATE_PEM = None

if os.path.exists(SECRETS_PATH):
    with open(f"{SECRETS_PATH}/private.pem", "rb") as f:
        PRIVATE_PEM = f.read()


router = Router()
logger = logging.getLogger(__name__)


# (NEW) если есть Django settings — используем BASE_DIR для абсолютных путей
try:
    from django.conf import settings
    BASE_DIR = Path(settings.BASE_DIR)
except Exception:
    BASE_DIR = Path(__file__).resolve().parents[2]  # fallback


# --- Прописываем пути к файлам снапшота---
CFG_PATH = (BASE_DIR / "connector/snapshots/test-connector/mapping.yml").resolve()
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

STORAGE_ROOT = (BASE_DIR / CFG["storage"]["root"]).resolve()
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

# переменная определяет наличие соединения к duckdb
_DB = None


def get_db():
    """Ленивое подключение к DuckDB (всегда файл, не :memory:)"""
    global _DB
    if _DB is None:
        _DB = duckdb.connect()
        logger.info(f"Connected to DuckDB")
    return _DB


@router.get("/v1/check-hash", response={200: str, 400: str}, auth=JWTAuth())
def check_hash(request):
    # проверка подписи
    schemas = CFG["schemas"]
    manifest_file = STORAGE_ROOT / CFG["storage"].get("manifest", "manifest.json")

    if manifest_file.exists():
        try:
            manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
            hashes_dict = manifest.get("hashes", {})
            for schema in schemas:
                parquet_file_name = schemas[schema].get('path')
                parquet_file_path = STORAGE_ROOT / parquet_file_name
                exp = hashes_dict.get(parquet_file_name).split(":", 1)[1]
                real = hashlib.sha256(parquet_file_path.read_bytes()).hexdigest()
                if real != exp:
                    return 400, 'Хэши не совпадают'
                return 200, 'Хэши совпали'

        except Exception as e:
            logger.warning(f"Failed to read manifest.json: {e}")


@router.post("/v1/lookup", response={200: dict, 400: str}, auth=JWTAuth())
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
    response = {
        "source_id": source_id,
        "status": "ok",
        "source_status": "live",
        "latency_ms": latency,
        "data": data
    }

    jws_token = jwt.encode(response, PRIVATE_PEM, algorithm="RS256")

    return 200, {"jwt": jws_token}


# --- API endpoints ---
@router.get('/get-info', response={200: str, 400: str})
def get_list(request):
    return 200, 'good connect'