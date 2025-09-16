from ninja import Router, Query, Body
import duckdb, hashlib, time, json
import yaml
from pathlib import Path
from .utils import build_sql
from django.http import JsonResponse
from ninja_jwt.authentication import JWTAuth

router = Router()


@router.get('/get-info', response={200: str, 400: str})
def get_list(request):
    return 200, 'good connect'

# загружаем содержимое mapping.yml
CFG_PATH = Path("connector/snapshots/test-connector/mapping.yml")
CFG = yaml.safe_load(Path(CFG_PATH).read_text(encoding="utf-8"))

# загружаем путь к manifest.json
STORAGE_ROOT = Path(CFG["storage"]["root"])
MANIFEST_FILE = STORAGE_ROOT / CFG["storage"].get("manifest", "")

DB = duckdb.connect(STORAGE_ROOT / "_cache.duckdb")

# --- Инициализация снапшота ---
# --- Создается база в duckdb ---
def init_snapshot():
    manifest = {}
    if MANIFEST_FILE.exists():
        manifest = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))

    for schema_name, schema_cfg in CFG.get("schemas", {}).items():
        file_path = STORAGE_ROOT / schema_cfg["path"]

        # проверка хэша
        h = manifest.get("hashes", {}).get(schema_cfg["path"])
        if h and h.startswith("sha256:"):
            exp = h.split(":", 1)[1]
            real = hashlib.sha256(file_path.read_bytes()).hexdigest()
            if real != exp:
                raise RuntimeError(f"Hash mismatch for {file_path.name}")

        # создаём таблицу в DuckDB
        DB.execute(f"DROP TABLE IF EXISTS {schema_name}")
        if file_path.suffix.lower() == ".parquet":
            DB.execute(f"CREATE TABLE {schema_name} AS SELECT * FROM read_parquet('{str(file_path)}')")
        else:
            DB.execute(f"CREATE TABLE {schema_name} AS SELECT * FROM read_csv_auto('{str(file_path)}', header=true)")

init_snapshot()

# --- Функция проверки JWS (заглушка) ---
def verify_jws(jws: str, body: bytes) -> bool:
    return bool(jws)


# --- API endpoint ---
@router.post("/v1/lookup", auth=JWTAuth())
def lookup(request, payload: dict = Body(...)):

    source_id = payload.get("source_id", "CARS")
    subject   = payload.get("subject", {})
    requested_groups = payload.get("requested_fields", []) or []

    start = time.time()
    data = {}

    for group_name in requested_groups:
        group_cfg = CFG["groups"].get(group_name)
        if not group_cfg:
            continue
        sql = build_sql(group_cfg, subject)

        print(sql)
        data[group_name] = DB.execute(sql).fetch_arrow_table().to_pylist()

    latency = int((time.time() - start) * 1000)
    return {
        "source_id": source_id,
        "status": "ok",
        "source_status": "live",
        "latency_ms": latency,
        "data": data
    }