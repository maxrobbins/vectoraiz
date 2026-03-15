"""
Tests for DatabaseConnector — SQL validation, type mapping, credential encryption.

Phase: BQ-VZ-DB-CONNECT
"""

import datetime
import decimal
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.services.db_connector import DatabaseConnector, get_db_connector


# =====================================================================
# SQL validation tests (20+ attack patterns)
# =====================================================================

class TestSQLValidation:
    """Mandate M4: Validate that only SELECT queries pass."""

    def test_valid_select(self):
        DatabaseConnector.validate_readonly_sql("SELECT * FROM users")

    def test_valid_select_with_where(self):
        DatabaseConnector.validate_readonly_sql("SELECT id, name FROM users WHERE active = true")

    def test_valid_select_with_join(self):
        DatabaseConnector.validate_readonly_sql(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
        )

    def test_valid_cte(self):
        DatabaseConnector.validate_readonly_sql(
            "WITH active_users AS (SELECT * FROM users WHERE active = true) "
            "SELECT * FROM active_users"
        )

    def test_valid_subquery(self):
        DatabaseConnector.validate_readonly_sql(
            "SELECT * FROM (SELECT id, name FROM users) sub"
        )

    def test_valid_aggregate(self):
        DatabaseConnector.validate_readonly_sql(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5"
        )

    def test_valid_union(self):
        DatabaseConnector.validate_readonly_sql(
            "SELECT id FROM users UNION ALL SELECT id FROM admins"
        )

    # --- Blocked patterns ---

    def test_reject_insert(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("INSERT INTO users (name) VALUES ('hacker')")

    def test_reject_update(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("UPDATE users SET admin = true WHERE id = 1")

    def test_reject_delete(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("DELETE FROM users WHERE id = 1")

    def test_reject_drop(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("DROP TABLE users")

    def test_reject_alter(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("ALTER TABLE users ADD COLUMN pwned TEXT")

    def test_reject_truncate(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("TRUNCATE TABLE users")

    def test_reject_create(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("CREATE TABLE evil (id INT)")

    def test_reject_grant(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("GRANT ALL ON users TO public")

    def test_reject_copy(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("COPY users TO '/tmp/dump.csv'")

    def test_reject_into_outfile(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT * FROM users INTO OUTFILE '/tmp/dump'")

    def test_reject_into_dumpfile(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT * FROM users INTO DUMPFILE '/tmp/dump'")

    def test_reject_load_file(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT LOAD_FILE('/etc/passwd')")

    def test_reject_pg_sleep(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT pg_sleep(10)")

    def test_reject_mysql_sleep(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT SLEEP(10)")

    def test_reject_benchmark(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT BENCHMARK(1000000, SHA1('test'))")

    def test_reject_dblink(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT * FROM dblink('host=evil', 'SELECT 1')")

    def test_reject_pg_read_file(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT pg_read_file('/etc/passwd')")

    def test_reject_empty_query(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("")

    def test_reject_whitespace_only(self):
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("   ")

    def test_reject_multi_statement_injection(self):
        """Reject SELECT followed by semicolon + malicious statement."""
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT 1; DROP TABLE users")

    def test_reject_select_into(self):
        """Reject SELECT INTO (creates a new table in some DBs)."""
        with pytest.raises(ValueError):
            DatabaseConnector.validate_readonly_sql("SELECT * INTO new_table FROM users")


# =====================================================================
# Type mapping tests (Mandate M2)
# =====================================================================

class TestTypeMapping:
    """Mandate M2: Verify Arrow type conversions."""

    def test_int_column(self):
        arr, field = DatabaseConnector._to_arrow_column("id", [1, 2, 3])
        assert arr.type == pa.int64()
        assert field.name == "id"

    def test_float_column(self):
        arr, field = DatabaseConnector._to_arrow_column("score", [1.5, 2.5, None])
        assert arr.type == pa.float64()

    def test_bool_column(self):
        arr, field = DatabaseConnector._to_arrow_column("active", [True, False, True])
        assert arr.type == pa.bool_()

    def test_string_column(self):
        arr, field = DatabaseConnector._to_arrow_column("name", ["alice", "bob"])
        assert arr.type == pa.string()

    def test_decimal_to_float64(self):
        """M2: DECIMAL → FLOAT64."""
        arr, field = DatabaseConnector._to_arrow_column(
            "amount", [decimal.Decimal("123.45"), decimal.Decimal("0.01"), None]
        )
        assert arr.type == pa.float64()
        assert arr[0].as_py() == pytest.approx(123.45)

    def test_uuid_to_text(self):
        """M2: UUID → TEXT."""
        u = uuid.uuid4()
        arr, field = DatabaseConnector._to_arrow_column("uid", [u, None])
        assert arr.type == pa.string()
        assert arr[0].as_py() == str(u)

    def test_json_to_text(self):
        """M2: JSON/JSONB (dict) → TEXT."""
        arr, field = DatabaseConnector._to_arrow_column("data", [{"key": "val"}, None])
        assert arr.type == pa.string()
        assert '"key"' in arr[0].as_py()

    def test_array_to_text(self):
        """M2: ARRAY (list) → TEXT."""
        arr, field = DatabaseConnector._to_arrow_column("tags", [[1, 2, 3], None])
        assert arr.type == pa.string()

    def test_bytea_skip(self):
        """M2: BYTEA → skip column."""
        arr, field = DatabaseConnector._to_arrow_column("blob", [b"\x00\x01", b"\x02"])
        assert arr is None
        assert field is None

    def test_datetime_utc(self):
        """M2: TIMESTAMPTZ → UTC."""
        import pytz
        eastern = pytz.timezone("US/Eastern")
        dt_eastern = eastern.localize(datetime.datetime(2024, 6, 15, 12, 0, 0))
        arr, field = DatabaseConnector._to_arrow_column("ts", [dt_eastern])
        assert arr.type == pa.timestamp("us")
        # Should be converted to UTC (16:00 UTC)
        result = arr[0].as_py()
        assert result.hour == 16

    def test_date_column(self):
        arr, field = DatabaseConnector._to_arrow_column("d", [datetime.date(2024, 1, 1)])
        assert arr.type == pa.date32()

    def test_all_none_column(self):
        arr, field = DatabaseConnector._to_arrow_column("empty", [None, None, None])
        assert arr.type == pa.string()

    def test_empty_values(self):
        arr, field = DatabaseConnector._to_arrow_column("empty", [])
        assert arr.type == pa.string()

    def test_enum_to_text(self):
        """M2: ENUM (string) → TEXT."""
        arr, field = DatabaseConnector._to_arrow_column("status", ["active", "inactive"])
        assert arr.type == pa.string()


# =====================================================================
# Credential encryption round-trip
# =====================================================================

class TestCredentialEncryption:
    """Verify password encryption and decryption."""

    def test_encrypt_decrypt_roundtrip(self):
        from app.services.db_credential_service import encrypt_password, decrypt_password

        plaintext = "super_secret_p@ssw0rd!#$%"
        token = encrypt_password(plaintext)
        assert token != plaintext
        assert decrypt_password(token) == plaintext

    def test_different_passwords_different_tokens(self):
        from app.services.db_credential_service import encrypt_password

        t1 = encrypt_password("password1")
        t2 = encrypt_password("password2")
        assert t1 != t2

    def test_encrypt_empty_string(self):
        """Edge case: empty password (shouldn't happen but shouldn't crash)."""
        from app.services.db_credential_service import encrypt_password, decrypt_password

        token = encrypt_password("")
        assert decrypt_password(token) == ""

    def test_encrypt_unicode(self):
        from app.services.db_credential_service import encrypt_password, decrypt_password

        plaintext = "пароль密码パスワード"
        token = encrypt_password(plaintext)
        assert decrypt_password(token) == plaintext


# =====================================================================
# Singleton
# =====================================================================

# =====================================================================
# Bulk introspection tests
# =====================================================================

class TestBulkIntrospection:
    """Verify bulk introspection returns the same structure as per-table."""

    def _make_mock_connection(self, db_type="postgresql"):
        conn = MagicMock()
        conn.id = "test-conn-1"
        conn.db_type = db_type
        conn.host = "localhost"
        conn.port = 5432 if db_type == "postgresql" else 3306
        conn.database = "testdb"
        conn.username = "user"
        conn.password_encrypted = "encrypted"
        conn.ssl_mode = "prefer"
        return conn

    @patch.object(DatabaseConnector, "get_engine")
    def test_bulk_pg_returns_table_info_list(self, mock_get_engine):
        """Bulk PG introspection returns List[TableInfo] with correct structure."""
        mock_conn_obj = MagicMock()
        mock_get_engine.return_value = MagicMock()

        # Mock the connection context manager and execute calls
        mock_db_conn = MagicMock()
        mock_get_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
        mock_get_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Setup execute results for the 3 bulk queries
        columns_result = [
            ("users", "id", "integer", "NO"),
            ("users", "name", "character varying", "YES"),
            ("users", "email", "character varying", "NO"),
            ("orders", "id", "integer", "NO"),
            ("orders", "user_id", "integer", "YES"),
            ("orders", "total", "numeric", "YES"),
        ]
        pk_result = [
            ("users", "id"),
            ("orders", "id"),
        ]
        count_result = [
            ("users", 1500),
            ("orders", 42000),
        ]

        mock_db_conn.execute.return_value.fetchall.side_effect = [
            columns_result, pk_result, count_result
        ]

        connector = DatabaseConnector()
        connection = self._make_mock_connection("postgresql")
        tables = connector._bulk_introspect_pg(connection, "public")

        assert len(tables) == 2
        table_names = {t.name for t in tables}
        assert table_names == {"users", "orders"}

        # Verify structure matches to_dict() contract
        for t in tables:
            d = t.to_dict()
            assert "name" in d
            assert "schema" in d
            assert "columns" in d
            assert "primary_key" in d
            assert "estimated_rows" in d
            assert d["schema"] == "public"
            # Columns should have name, type, nullable
            for col in d["columns"]:
                assert "name" in col
                assert "type" in col
                assert "nullable" in col

        # Check specific table
        users = next(t for t in tables if t.name == "users")
        assert len(users.columns) == 3
        assert users.estimated_rows == 1500
        assert users.primary_key == {"constrained_columns": ["id"]}

        orders = next(t for t in tables if t.name == "orders")
        assert len(orders.columns) == 3
        assert orders.estimated_rows == 42000

    @patch.object(DatabaseConnector, "get_engine")
    def test_bulk_mysql_returns_table_info_list(self, mock_get_engine):
        """Bulk MySQL introspection returns List[TableInfo] with correct structure."""
        mock_db_conn = MagicMock()
        mock_get_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
        mock_get_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)

        # First execute is SELECT DATABASE()
        mock_db_conn.execute.return_value.scalar.return_value = "testdb"

        columns_result = [
            ("products", "id", "int", "NO"),
            ("products", "name", "varchar", "YES"),
        ]
        pk_result = [("products", "id")]
        count_result = [("products", 500)]

        mock_db_conn.execute.return_value.fetchall.side_effect = [
            columns_result, pk_result, count_result
        ]

        connector = DatabaseConnector()
        connection = self._make_mock_connection("mysql")
        tables = connector._bulk_introspect_mysql(connection, None)

        assert len(tables) == 1
        assert tables[0].name == "products"
        assert tables[0].estimated_rows == 500
        d = tables[0].to_dict()
        assert len(d["columns"]) == 2
        assert d["columns"][0]["name"] == "id"
        assert d["columns"][0]["nullable"] is False

    @patch.object(DatabaseConnector, "_per_table_introspect")
    @patch.object(DatabaseConnector, "_bulk_introspect_pg")
    @patch.object(DatabaseConnector, "get_engine")
    def test_fallback_to_per_table_on_bulk_failure(self, mock_engine, mock_bulk, mock_per_table):
        """If bulk introspection fails, fall back to per-table approach."""
        mock_bulk.side_effect = Exception("bulk query failed")
        mock_per_table.return_value = []

        connector = DatabaseConnector()
        connection = self._make_mock_connection("postgresql")
        result = connector.introspect_schema(connection, "public")

        mock_per_table.assert_called_once_with(connection, "public")
        assert result == []

    @patch.object(DatabaseConnector, "get_engine")
    def test_bulk_pg_table_without_pk(self, mock_get_engine):
        """Tables without primary keys should have primary_key=None."""
        mock_db_conn = MagicMock()
        mock_get_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
        mock_get_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)

        columns_result = [("logs", "message", "text", "YES")]
        pk_result = []  # no PKs
        count_result = [("logs", 100)]

        mock_db_conn.execute.return_value.fetchall.side_effect = [
            columns_result, pk_result, count_result
        ]

        connector = DatabaseConnector()
        connection = self._make_mock_connection("postgresql")
        tables = connector._bulk_introspect_pg(connection, "public")

        assert len(tables) == 1
        assert tables[0].primary_key is None


class TestSingleton:
    def test_get_db_connector_returns_same_instance(self):
        c1 = get_db_connector()
        c2 = get_db_connector()
        assert c1 is c2
