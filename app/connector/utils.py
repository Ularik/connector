from jose import jwt


def sql_select_only(group_cfg):
    from_cfg = group_cfg["from"]
    schema = from_cfg["schema"]  # название таблицы

    # SELECT car.car_id AS car_id, user.name AS user_name ...
    select_map = from_cfg["select"]
    cols = ", ".join([f"{src} AS {alias}" for alias, src in select_map.items()])

    sql_only_select = f"SELECT {cols} FROM {schema}"
    return sql_only_select


def build_sql(group_cfg, subject: dict) -> str:
    sql_only_select = sql_select_only(group_cfg)
    from_cfg = group_cfg["from"]

    # JOIN
    if "join" in from_cfg:
        for j in from_cfg["join"]:
            sql_only_select += f" JOIN {j['schema']} ON {j['on']}"

    # WHERE
    conditions = []
    for field, path in from_cfg["where_any"].items():
        if field in subject:
            val = subject[field]
            if isinstance(val, str):
                val = f"'{val}'"
            conditions.append(f"{field} = {val}")

    if not conditions:
        return None

    sql = sql_only_select + " WHERE " + " AND ".join(conditions)
    return sql


def build_sql_like(group_cfg, subject: dict) -> str:
    sql_only_select = sql_select_only(group_cfg)
    from_cfg = group_cfg['from']

    # JOIN
    if "join" in from_cfg:
        for j in from_cfg["join"]:
            sql_only_select += f" JOIN {j['schema']} ON {j['on']}"

    # WHERE
    conditions = []
    if "where_any" in from_cfg:
        for field, path in from_cfg["where_any"].items():
            if field in subject:
                val = subject[field]
                if isinstance(val, str):
                    val = f"{val}"
                conditions.append(f"{field} LIKE '%{val}%'")

    sql = sql_only_select + " WHERE " + " AND ".join(conditions)
    return sql