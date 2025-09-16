def build_sql(group_cfg, subject: dict) -> str:
    """
    group_cfg = {
        "from": {
            "schema": "car",
            "join": [
                {"schema": "user", "on": "car.user_id = user.user_id"}
            ],
            "where_any": {
                "car_id": "$.subject.car_id"
            }
        },
        "select": {
            "car_id": "car.car_id",
            "title": "car.title",
            "color": "car.color",
            "full_name": "user.full_name",
            "user_age": "user.age"
        }
    }
    """
    from_cfg = group_cfg["from"]
    schema = from_cfg["schema"]    # название таблицы

    # SELECT car.car_id AS car_id, user.name AS user_name ...
    select_map = from_cfg["select"]
    cols = ", ".join([f"{src} AS {alias}" for alias, src in select_map.items()])

    sql = f"SELECT {cols} FROM {schema}"

    # JOIN
    if "join" in from_cfg:
        for j in from_cfg["join"]:
            sql += f" JOIN {j['schema']} ON {j['on']}"

    # WHERE
    conditions = []
    if "where_any" in from_cfg:
        for field, path in from_cfg["where_any"].items():
            if field in subject:
                val = subject[field]
                if isinstance(val, str):
                    val = f"'{val}'"
                conditions.append(f"{field} = {val}")

    if conditions:
        sql += " WHERE " + " OR ".join(conditions)

    return sql
