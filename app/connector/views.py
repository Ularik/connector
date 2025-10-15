from ninja import Router, Body
import duckdb, hashlib, time, json, yaml, logging
from pathlib import Path
from .utils import build_sql, build_sql_like
from ninja_jwt.authentication import JWTAuth
from jose import jwt
import os
from project.settings_local import SNAPSHOT_PATH, SECRETS_PATH
import datetime
import base64


router = Router()
logger = logging.getLogger(__name__)


# --- Прописываем пути к файлам снапшота---
CFG_PATH = Path(f"{SNAPSHOT_PATH}/mapping.yml").resolve()
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

STORAGE_ROOT = Path(CFG["storage"]["root"])
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
    """
    Сверяет хэши файлов баз parquet с указанными в manifest
    """
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


@router.post("/v1/lookup-like", response={200: dict, 400: str}, auth=JWTAuth())
def lookup_like(request, payload: dict = Body(...)):
    """
    Пример тела запроса
    {
      "source_id": "CARS",
      "subject": { "gov_plate": "01KG517AUF" },
      "requested_fields": ["vehicles"]
    }
    """

    source_id = payload.get("source_id", "CARS")
    subject = payload.get("subject", {})
    requested_groups = payload.get("requested_fields", []) or []

    start = time.time()
    data = {}
    con = get_db()

    group_name = requested_groups[0]
    group_cfg = CFG.get("groups", {}).get(group_name)  # выбираем нужную группу из "groups" по названию "group_name"
    if not group_cfg:
        logger.info(f'Такой группы в mapping.yml нет: {group_name}')
        return 400, f'Такой группы в mapping.yml нет: {group_name}'

    sql = build_sql_like(group_cfg, subject)  # готовая sql команда для базы

    ### адаптируем sql для обращения к parquet файлу ###
    sql_parquet = sql_convert_parquet(group_cfg, sql)

    ### обращаемся к parquet файлу и получаем данные ###
    group_data = con.execute(sql_parquet).fetch_arrow_table().to_pylist()
    for row in group_data:
        for k, v in row.items():
            if isinstance(v, (datetime.date, datetime.datetime)):
                row[k] = v.isoformat()

    data[group_name] = group_data

    latency = int((time.time() - start) * 1000)
    response = {
        "source_id": source_id,
        "status": "ok",
        "source_status": "live",
        "latency_ms": latency,
        "data": data
    }

    response = jwt_encode_service(response)
    return 200, response


@router.post("/v1/lookup", auth=JWTAuth())
def lookup(request, payload: dict = Body(...)):
    """
    Пример тела запроса:
    {
      "source_id": "CARS",
      "subject": { "gov_plate": "01KG517AUF" },
      "requested_fields": ["vehicles"],
      "page": 1,
      "page_size": 500
    }
    """

    source_id = payload.get("source_id", "CARS")
    subject = {k: v for k, v in payload.get("subject", {}).items() if bool(v)}
    requested_groups = payload.get("requested_fields", []) or []

    page = int(payload.get("page", 1))
    page_size = int(payload.get("page_size", 500))
    offset = (page - 1) * page_size

    start = time.time()
    data = {}
    con = get_db()

    for group_name in requested_groups:
        group_cfg = CFG.get("groups", {}).get(group_name)
        if not group_cfg:
            logger.info(f'Такой группы в mapping.yml нет: {group_name}')
            continue

        sql = build_sql(group_cfg, subject)
        sql_parquet = sql_convert_parquet(group_cfg, sql)

        # Подсчёт общего количества строк
        count_sql = f"SELECT COUNT(*) AS total FROM ({sql_parquet})"
        total_rows = con.execute(count_sql).fetchone()[0]

        # Добавляем пагинацию
        paginated_sql = f"{sql_parquet} LIMIT {page_size} OFFSET {offset}"
        # Получаем только текущую страницу
        result = con.execute(paginated_sql).fetch_arrow_table()
        group_data = []
        for batch in result.to_batches():
            for row in batch.to_pylist():
                for k, v in row.items():
                    if isinstance(v, (datetime.date, datetime.datetime)):
                        row[k] = v.isoformat()
                    elif k in ('photo', 'signature') and isinstance(v, (bytes, bytearray)):
                        row[k] = base64.b64encode(v).decode("utf-8")
                group_data.append(row)

        # Добавляем метаданные о пагинации
        data[group_name] = {
            "total": total_rows,
            "page": page,
            "page_size": page_size,
            "has_next": offset + page_size < total_rows,
            "results": group_data
        }

    latency = int((time.time() - start) * 1000)
    response = {
        "source_id": source_id,
        "status": "ok",
        "source_status": "live",
        "latency_ms": latency,
        "data": data
    }

    return jwt_encode_service(response)


def sql_convert_parquet(group_cfg, sql) -> str:
    """
    Получает голый sql запрос,
    Возвращает запрос к файлам 'parquet' на основе путей из group_cfg
    """
    schema = group_cfg['from']
    schema_name = schema['schema']
    join_schema = schema.get('join')

    # schema_name
    schema_cfg = CFG.get("schemas").get(schema_name)
    parquet_file = schema_cfg.get('path')
    parquet_path = (STORAGE_ROOT / parquet_file).resolve()

    sql = sql.replace(f"FROM {schema_name}", f"FROM read_parquet('{parquet_path}') AS {schema_name}")
    # если есть JOIN в группе group_cfg
    if join_schema:
        join_schema_name = join_schema[0].get("schema")
        schema_cfg = CFG.get("schemas").get(join_schema_name)
        parquet_file = schema_cfg.get("path")
        parquet_path = (STORAGE_ROOT / parquet_file).resolve()
        if join_schema_name == 'document_images':
            sql = sql.replace(f"JOIN {join_schema_name}", f"JOIN '{parquet_path}/*.parquet' AS {join_schema_name}")
        else:
            sql = sql.replace(f"JOIN {join_schema_name}", f"JOIN read_parquet('{parquet_path}') AS {join_schema_name}")

    return sql


@router.post('/get-photo', response={200: dict, 400: str}, auth=JWTAuth())
def get_images(request, payload: dict = Body(...)):  # 100009209574
    source_id = payload.get("source_id", "CARS")
    subject = payload.get("subject", {})
    requested_groups = payload.get("requested_fields", []) or []

    start = time.time()
    data = {}
    con = get_db()

    if 'document_images' in requested_groups:

        group_name = requested_groups[0]
        group_cfg = CFG.get("groups", {}).get(group_name)  # выбираем нужную группу из "groups" по названию "group_name"
        if not group_cfg:
            logger.info(f'Такой группы в mapping.yml нет: {group_name}')
            return 400, f'Такой группы в mapping.yml нет: {group_name}'

        sql = build_sql(group_cfg, subject)  # готовая sql команда для базы

        ### адаптируем sql для обращения к parquet файлу ###
        schema_name = group_cfg['from']['schema']
        schema_cfg = CFG.get("schemas").get(schema_name)
        parquet_file = schema_cfg['path']
        parquet_path = (STORAGE_ROOT / parquet_file).resolve()
        sql_parquet = sql.replace(f"FROM {schema_name}", f"FROM '{parquet_path}/*.parquet' AS {schema_name}")

        ### обращаемся к parquet файлу и получаем данные ###
        group_data = con.execute(sql_parquet).fetch_arrow_table().to_pylist()
        for row in group_data:
            for k, v in row.items():
                if k in ('photo', 'signature'):
                    encoded_photo = base64.b64encode(v).decode("utf-8")
                    row[k] = encoded_photo

        data[group_name] = group_data

    latency = int((time.time() - start) * 1000)
    response = {
        "source_id": source_id,
        "status": "ok",
        "source_status": "live",
        "latency_ms": latency,
        "data": data
    }

    response = jwt_encode_service(response)

    return response


def jwt_encode_service(body):
    """
    преобразует тело в jwt строку
    """

    if not SECRETS_PATH:
        return body

    private_pem_path = f"{SECRETS_PATH}/private.pem"
    if os.path.exists(private_pem_path):
        with open(private_pem_path, "rb") as f:
            private_pem = f.read()
            jws_token = jwt.encode(body, private_pem, algorithm="RS256")
        return {"jwt": jws_token}

    return body

