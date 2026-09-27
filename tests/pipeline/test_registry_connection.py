"""Control-plane connection behavior during overlapping pipeline stages."""

from ai_trading_system.pipeline.registry import RegistryStore


def test_registry_reader_can_open_while_writer_is_connected(tmp_path):
    registry = RegistryStore(
        tmp_path, db_path=tmp_path / "control_plane.duckdb", initialize=False
    )
    with registry._writer() as conn:
        conn.execute("CREATE TABLE sample (id INTEGER)")

    with registry._writer() as writer:
        writer.execute("BEGIN TRANSACTION")
        writer.execute("INSERT INTO sample VALUES (1)")
        with registry._reader() as reader:
            # The concurrent reader sees only committed data, without a
            # connection-configuration error.
            assert reader.execute("SELECT COUNT(*) FROM sample").fetchone() == (0,)

    with registry._reader() as reader:
        assert reader.execute("SELECT COUNT(*) FROM sample").fetchone() == (1,)
