import os
import sys
from typing import Generator

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinQueryError
from harlequin_athena.adapter import HarlequinAthenaAdapter, HarlequinAthenaConnection
from textual_fastdatatable.backend import create_backend

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

TEST_DB = "default"


@pytest.fixture
def athena_options() -> dict:
    workgroup = os.getenv("AWS_ATHENA_WORKGROUP")
    assert workgroup, "Required environment variable `AWS_ATHENA_WORKGROUP` not found."
    s3_staging_dir = os.getenv("AWS_ATHENA_S3_STAGING_DIR")
    assert (
        s3_staging_dir
    ), "Required environment variable `AWS_ATHENA_S3_STAGING_DIR` not found."
    region_name = os.getenv("AWS_DEFAULT_REGION")
    assert region_name, "Required environment variable `AWS_DEFAULT_REGION` not found."
    return {
        "work_group": workgroup,
        "s3_staging_dir": None,
        "region_name": region_name,
        "result_reuse_enable": False,
        "result_reuse_minutes": 69,
        "use_glue_catalog": True,
        "unload": False,
    }


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "athena"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinAthenaAdapter


def test_connect(athena_options: dict) -> None:
    conn = HarlequinAthenaAdapter(**athena_options).connect()
    assert isinstance(conn, HarlequinConnection)


@pytest.fixture
def connection(
    athena_options: dict,
) -> Generator[HarlequinAthenaConnection, None, None]:
    conn = HarlequinAthenaAdapter(**athena_options).connect()
    yield conn


def test_get_catalog(connection: HarlequinAthenaConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_execute_ddl(connection: HarlequinAthenaConnection) -> None:
    cur = connection.execute(
        f"""
        CREATE EXTERNAL TABLE awsdatacatalog.{TEST_DB}.my_table (
            a int
        )
        LOCATION 's3://nx-ds-sandbox-live-prd-nexiot/ephemeral/harlequin/default/my_table/'"""
    )
    assert cur is not None
    data = cur.fetchall()
    assert not data
    cur2 = connection.execute(f"DROP TABLE awsdatacatalog.{TEST_DB}.my_table PURGE")
    drop_data = cur2.fetchall()
    assert not drop_data


def test_execute_select(connection: HarlequinAthenaConnection) -> None:
    cur = connection.execute("select 1 as a")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [("a", "#")]
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_execute_select_dupe_cols(connection: HarlequinAthenaConnection) -> None:
    cur = connection.execute("select 1 as a, 2 as a, 3 as a")
    assert isinstance(cur, HarlequinCursor)
    assert len(cur.columns()) == 3
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1


# PyAthena does not support duplicated columns when using fetchmany()
def test_execute_select_dupe_cols_fetchmany(
    connection: HarlequinAthenaConnection,
) -> None:
    cur = connection.execute("select 1 as a, 2 as a, 3 as a")
    assert isinstance(cur, HarlequinCursor)
    assert len(cur.columns()) == 3
    cur.set_limit(5)
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_set_limit(connection: HarlequinAthenaConnection) -> None:
    cur = connection.execute("select 1 as a union all select 2 union all select 3")
    assert isinstance(cur, HarlequinCursor)
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(connection: HarlequinAthenaConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection.execute("selec;")
