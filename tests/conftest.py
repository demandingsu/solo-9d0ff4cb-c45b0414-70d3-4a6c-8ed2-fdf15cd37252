"""pytest 夹具：每个用例独立临时 SQLite 库 + FastAPI TestClient。"""
import sys
import os
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture()
def client(monkeypatch):
    tmpdir = tempfile.mkdtemp(prefix="discharge-test-")
    db_path = os.path.join(tmpdir, "test.db")
    monkeypatch.setenv("DB_PATH", db_path)

    # 延迟导入，确保 DB_PATH 环境变量先生效
    from app import database
    from app.main import app
    from fastapi.testclient import TestClient

    monkeypatch.setattr(database, "DB_PATH", db_path)
    database.init_db(db_path)

    # raise_server_exceptions=False：让兜底 500 处理器按真实 HTTP 行为返回响应
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
