import sqlalchemy
import clickhouse_connect

FUTURE_DB = sqlalchemy.create_engine("clickhouse://default:Tt1234567890@172.30.115.157:8123/future")
FUTURE_DB_ORIGIN = clickhouse_connect.get_client(
    host="172.30.115.157",
    port=8123,
    user="default",
    password="Tt1234567890",
    database="future"
)