from pprint import pprint
import re


# def sql_select_only(group_cfg):
#     from_cfg = group_cfg["from"]
#     schema = from_cfg["schema"]  # название таблицы
#
#     select_map = from_cfg.get("select")
#
#     cols = []
#     for alias, src in select_map.items():
#         if alias in ('photo', 'signature'):
#             cols.append(f"{alias}:=base64({src})")
#         else:
#             cols.append(f"{src} AS {alias}")
#
#     if 'join' in from_cfg:    # добавляем все поля из зависимых таблиц как отдельное поле главной таблицы
#         joins = from_cfg.get('join')
#         for join_schema in joins:
#             if join_schema['schema'] == 'document_images':
#                 cols.append(f"json_group_array(json_object('formular_number', document_images.formular_number, 'photo', base64(document_images.photo))) AS {join_schema['schema']}")
#             else:
#                 cols.append(f"json_group_array(to_json({join_schema['schema']})) AS {join_schema['schema']}")
#
#     cols = ", ".join(cols)
#     sql_only_select = f"SELECT {cols} FROM {schema}"
#     return sql_only_select
#
#
# def build_sql(group_cfg, subject: dict) -> str:
#     sql_only_select = sql_select_only(group_cfg)
#     from_cfg = group_cfg["from"]
#
#     # JOIN
#     if "join" in from_cfg:
#         for j in from_cfg["join"]:
#             sql_only_select += f" JOIN {j['schema']} ON {j['on']}"
#
#     # WHERE
#     conditions = []
#     for field, path in from_cfg["where_any"].items():
#         if field in subject:
#             val = subject[field]
#             if bool(re.search(r'^\d{4}(-\d{2}){0,2}$', val)) or val.isdigit():  # проверка на дату, для запроса к базе
#                 conditions.append(f"CAST({field} AS VARCHAR) LIKE '%{val}%'")
#             else:
#                 conditions.append(f"{field} LIKE '%{val}%'")
#
#     if not conditions:
#         return None
#
#     sql = sql_only_select + f" WHERE " + " AND ".join(conditions)
#
#     if "join" in from_cfg:
#         sql += "GROUP BY ALL"
#
#     return sql


def sql_select_only(group_cfg, subject):
    from_cfg = group_cfg["from"]
    schema = from_cfg["schema"]  # название таблицы

    # SELECT car.car_id AS car_id, user.name AS user_name ...
    select_map = from_cfg.get("select")

    join_schemas = {}
    if "join" in from_cfg:
        for j in from_cfg["join"]:
            index = j['on'].find(j['schema'])
            general_filed = j['on'][index + len(j['schema']) + 1:]
            join_schemas[j['schema']] = general_filed

    cols = []
    join_cols = {}
    for alias, src in select_map.items():
        join_schema_name = src.split('.')[0]
        if join_schema_name in join_schemas:
            if alias in ('photo', 'signature'):
                join_cols.setdefault(join_schema_name, []).append(f"{alias}:=base64({src})")
            else:
                join_cols.setdefault(join_schema_name, []).append(f"{alias}:={src}")
        elif join_schema_name == schema:
            cols.append(f"{src} AS {alias}")

    join_selects = {}
    for join_schema_name in join_cols:
        cols.append(join_schema_name)
        struct_pack = ', '.join(join_cols[join_schema_name])
        general_filed = join_schemas[join_schema_name]

        inner_select = (f"SELECT {general_filed}, "
                        f"ARRAY_AGG(DISTINCT struct_pack({struct_pack})) "
                        f"AS {join_schema_name} "
                        f"FROM {join_schema_name} ")

        filter_cols = []
        for up_field in subject:
            if join_schema_name in up_field:
                val = subject[up_field]
                low_field = up_field.split('.')[1]   # example: phone.phones -> phones
                filter_cols.append(f"{low_field} = {val} ")

        if filter_cols:
            inner_select += 'WHERE ' + ', '.join(filter_cols)

        inner_select +=  f"GROUP BY {general_filed}"
        join_selects[join_schema_name] = inner_select

    cols = ", ".join(cols)
    sql_only_select = f"SELECT {cols} FROM {schema}"
    return sql_only_select, join_selects


def build_sql(group_cfg, subject: dict) -> str:
    from_cfg = group_cfg["from"]
    join_subjects = {}

    # WHERE
    conditions = []
    for field, _ in from_cfg["where_any"].items():
        if field in subject:
            val = subject[field]

            if len(field.split('.')) == 2:
                join_subjects[field] = val
                continue

            if bool(re.search(r'^\d{4}(-\d{2}){0,2}$', val)):   # проверка на дату, для запроса к базе
                conditions.append(f"CAST({field} AS VARCHAR) LIKE '%{val}%'")
            else:
                conditions.append(f"{field} LIKE '%{val}%'")

    sql_only_select, join_selects = sql_select_only(group_cfg, join_subjects)

    # JOIN
    if "join" in from_cfg:
        for j in from_cfg["join"]:
            join_schema_name = j['schema']
            if join_schema_name not in join_selects:
                continue
            sql_only_select += f" LEFT JOIN ({join_selects[join_schema_name]}) {join_schema_name} ON {j['on']}"

    sql = sql_only_select
    if conditions:
        sql += f" WHERE " + " AND ".join(conditions)

    return sql
