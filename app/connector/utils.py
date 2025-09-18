from jose import jwt

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

#
# with open(f'secrets/public.pem', 'rb') as f:
#     public_pem = f.read()
#
# test = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzb3VyY2VfaWQiOiJDQVJTIiwic3RhdHVzIjoib2siLCJzb3VyY2Vfc3RhdHVzIjoibGl2ZSIsImxhdGVuY3lfbXMiOjEsImRhdGEiOnsidmVoaWNsZXMiOlt7ImlkIjoxLCJjb2xvciI6ImdyZWVuIiwidGl0bGUiOiJnb2xmIiwidXNlcl9pZCI6Mn1dfX0.Qqc0TciN0wlGLKJ6HhDwoRVmFxe0Wvx1Y5i9_rSYEFHHEDmMrKqXegvG8x2JCCYFrjZDirgvXOzXN1zCosYm57-vWV0IeZg6GlHAkrOzG3RE0584EoewLHv07A2FJrQVPuOc75g8EaC5hzgCyfazi2JABkIhDVXzN0n--J0akuL-OilcKDff57qYNCCIrQZ5cDgUlEFqy-x8L513GKgTiIWlzsBqhPa1_YW3Y_uvvMbDZBDOJeuz9tc_cL_QrVOdE-w5O9APdksC4UNBrBi9LhJ9EFv8eao4_O0Yvv-7K16CSpahTcUh8JdS0BeS51N8imOMq3VzU-LB90qepOUfjA'
# print(jwt.decode(test, public_pem, algorithms=["RS256"])
# )