from __future__ import annotations  # noqa: I001

from typing import Any

from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_athena.cli_options import ATHENA_OPTIONS
from harlequin_athena.completions import load_completions

from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor
import boto3


class HarlequinAthenaCursor(HarlequinCursor):
    def __init__(self, cur: Any) -> None:
        self.cur = cur
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        assert self.cur.description is not None
        return [
            (col[0], self.get_short_col_type(col[1])) for col in self.cur.description
        ]

    @staticmethod
    def get_short_col_type(type_name: str) -> str:
        MAPPING = {
            "array": "[]",
            "bigint": "##",
            "boolean": "t/f",
            "char": "s",
            "date": "d",
            "decimal": "#.#",
            "double": "#.#",
            "ipaddress": "ip",
            "integer": "#",
            "interval": "|-|",
            "json": "{}",
            "row": "{}",
            "real": "#.#",
            "smallint": "#",
            "time": "t",
            "timestamp": "ts",
            "tinyint": "#",
            "uuid": "uid",
            "varchar": "t",
            "string": "t",
            "struct": "{}",
        }
        return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")

    def set_limit(self, limit: int) -> HarlequinAthenaCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.as_arrow()
            else:
                return self.cur.fetchmany(self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        # finally:
        #     self.cur.close()


class HarlequinAthenaConnection(HarlequinConnection):
    def __init__(
        self,
        *_: Any,
        init_message: str = "",
        options: dict[str, Any],
    ) -> None:
        self.init_message = init_message
        _options = options.copy()
        _unload = _options.pop("unload")
        self.use_glue_catalog = _options.pop("use_glue_catalog")
        _options["cursor_class"] = ArrowCursor
        _options["cursor_kwargs"] = {"unload": _unload}
        try:
            self.conn = connect(**_options)
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to your database."
            ) from e

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            cur = self.conn.cursor().execute(query)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        return HarlequinAthenaCursor(cur)

    def get_catalog(self) -> Catalog:
        databases = self._get_databases()
        db_items: list[CatalogItem] = []
        glue_client = boto3.client("glue")
        for db in databases:
            tbls = glue_client.get_tables(DatabaseName=db)["TableList"]
            relations = self._get_relations(tbls)
            rel_items: list[CatalogItem] = []
            for rel in relations:
                rel_items.append(
                    CatalogItem(
                        qualified_identifier=f'"awsdatacatalog"."{db}"."{rel["rel_name"]}"',
                        query_name=f'"awsdatacatalog"."{db}"."{rel["rel_name"]}"',
                        label=rel["rel_name"],
                        type_label=rel["rel_type"],
                        children=self._get_columns(db, rel["rel_name"], rel["columns"]),
                    )
                )
            db_items.append(
                CatalogItem(
                    qualified_identifier=f'"awsdatacatalog"."{db}"',
                    query_name=f'"awsdatacatalog"."{db}"',
                    label=db,
                    type_label="s",
                    children=rel_items,
                )
            )
        catalog_items = [
            CatalogItem(
                qualified_identifier='"awsdatacatalog"',
                query_name='"awsdatacatalog"',
                label="awsdatacatalog",
                type_label="c",
                children=db_items,
            )
        ]
        return Catalog(items=catalog_items)

    def _get_columns(
        self,
        db: str,
        rel: str,
        cols: list[tuple(str, str)],
    ) -> list[CatalogItem]:
        return [
            CatalogItem(
                qualified_identifier=f'"awsdatacatalog"."{db}"."{rel}"."{col_name}"',
                query_name=f"{col_name}",
                label=col_name,
                type_label=self.get_short_col_type(col_type),
            )
            for col_name, col_type in cols
        ]

    def _get_relations(self, tbls: list[Any]) -> dict():
        relations = [
            {
                "rel_name": t["Name"],
                "rel_type": "t" if "TABLE" in t["TableType"] else "v",
                "columns": [
                    (
                        c["Name"],
                        c["Type"],
                    )
                    for c in t["StorageDescriptor"]["Columns"]
                ],
            }
            for t in tbls
        ]
        return relations

    def _get_databases(self) -> list[tuple[str]]:
        schemas = []
        if self.use_glue_catalog:
            glue_client = boto3.client("glue")
            schemas += [
                (db["Name"]) for db in glue_client.get_databases()["DatabaseList"]
            ]
        else:
            cur = self.conn.cursor()
            cur.execute("SHOW DATABASES")
            results = cur.fetchall()
            cur.close()
            schemas += [result for result in results]
        return schemas

    def get_completions(self) -> list[HarlequinCompletion]:
        return load_completions()

    @staticmethod
    def get_short_col_type(type_name: str) -> str:
        MAPPING = {
            "array": "[]",
            "bigint": "##",
            "boolean": "t/f",
            "char": "s",
            "date": "d",
            "decimal": "#.#",
            "double": "#.#",
            "ipaddress": "ip",
            "integer": "#",
            "interval": "|-|",
            "json": "{}",
            "row": "{}",
            "real": "#.#",
            "smallint": "#",
            "time": "t",
            "timestamp": "ts",
            "tinyint": "#",
            "uuid": "uid",
            "varchar": "t",
            "string": "t",
        }
        return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")


class HarlequinAthenaAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = ATHENA_OPTIONS

    def __init__(
        self,
        work_group: str | None = None,
        s3_staging_dir: str | None = None,
        result_reuse_enable: bool = True,
        result_reuse_minutes: str | None = 60,
        use_glue_catalog: bool = True,
        unload: bool = False,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        **_: Any,
    ) -> None:
        self.options = {
            "work_group": work_group,
            "s3_staging_dir": s3_staging_dir,
            "result_reuse_enable": result_reuse_enable,
            "result_reuse_minutes": int(result_reuse_minutes),
            "use_glue_catalog": use_glue_catalog,
            "unload": unload,
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
        }

    def connect(self) -> HarlequinAthenaConnection:
        conn = HarlequinAthenaConnection(options=self.options)
        return conn
