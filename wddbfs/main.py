# -*- coding: utf-8 -*-
# (c) 2009-2023 Martin Wendt and contributors; see WsgiDAV https://github.com/mar10/wsgidav
# (c) 2024-2024 Adam Obeng
# Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php

# TODO
# - write support
# - add caching based on file update time

import pandas as pd
import os
import io
from urllib.parse import quote

from wsgidav import util

from wsgidav.dav_provider import DAVCollection, DAVNonCollection, DAVProvider
from wsgidav.util import join_uri
import sqlite3
import json

__docformat__ = "reStructuredText en"

_logger = util.get_module_logger(__name__)

BUFFER_SIZE = 8192


class TableFormatter:
    def _fetch_query(self, query: str, connection: sqlite3.Connection) -> pd.DataFrame:
        cursor = connection.execute(query)
        columns = [d[0] for d in cursor.description]
        result = cursor.fetchall()
        if not isinstance(result, list):
            result = list(result)
        df = pd.io.sql._wrap_result(result, columns=columns)
        return df

    def __call__(self, query: str, connection: sqlite3.Connection, output: io.BytesIO):
        return


class CSVFormatter(TableFormatter):
    def __init__(self, **csv_kwargs):
        self.csv_kwargs = csv_kwargs

    def __call__(self, query: str, connection: sqlite3.Connection, output: io.BytesIO):
        df = self._fetch_query(query=query, connection=connection)
        df.to_csv(output, index=False, **self.csv_kwargs)


class JSONFormatter(TableFormatter):
    def __call__(self, query: str, connection: sqlite3.Connection, output: io.BytesIO):
        df = self._fetch_query(query=query, connection=connection)
        df.to_json(output, orient="records")


class JSONLFormatter(TableFormatter):
    def __call__(self, query: str, connection: sqlite3.Connection, output: io.BytesIO):
        df = self._fetch_query(query=query, connection=connection)
        output.write(
            "\n".join([json.dumps(r.to_dict()) for _, r in df.iterrows()]).encode(
                "utf8"
            )
        )


TABLE_FORMATTERS = {
    ".csv": CSVFormatter(),
    ".tsv": CSVFormatter(sep="\t"),
    ".json": JSONFormatter(),
    ".jsonl": JSONLFormatter(),
}


class PathCollection(DAVCollection):
    """used for recurstively finding database file by absolute path on host filesystem"""

    def __init__(self, path, environ, resource_provider, formats):
        super().__init__(path, environ)
        self.formats = formats
        self.resource_provider = resource_provider

    def get_member(self, name):
        p = join_uri(self.path, name)
        if os.path.exists(p):
            if os.path.isdir(p):
                return PathCollection(
                    p,
                    self.environ,
                    resource_provider=self.resource_provider,
                    formats=self.formats,
                )

            else:
                return DBCollection(
                    p,
                    environ=self.environ,
                    resource_provider=self.resource_provider,
                    formats=self.formats,
                )
        return None

    def get_member_names(self):
        return []


class RootCollection(DAVCollection):
    """Resolve top-level requests '/'."""

    def __init__(self, environ, resource_provider, formats=TABLE_FORMATTERS.keys()):
        self.resource_provider = resource_provider
        self.formats = formats
        super().__init__("/", environ)

    @property
    def _member_names(self):
        r = tuple(d.name for d in self.resource_provider.dbs)
        return r

    def get_member_names(self):
        r = self._member_names
        return r

    def get_member(self, name):
        if name in self._member_names:
            return DBCollection(
                path=join_uri(self.path, name),
                environ=self.environ,
                resource_provider=self.resource_provider,
                formats=self.formats,
            )
        elif self.resource_provider.allow_abspath and os.path.exists(
            join_uri("/", name)
        ):
            return PathCollection(
                join_uri("/", name),
                self.environ,
                resource_provider=self.resource_provider,
                formats=self.formats,
            )

        return None


class DBCollection(DAVCollection):
    """Top level database, contains tables"""

    # TOOD: support multiple databases per file

    def __init__(
        self, path, environ, resource_provider, formats=TABLE_FORMATTERS.keys()
    ):
        self.resource_provider = resource_provider
        self.formats = formats
        super().__init__(path, environ)

    def get_display_info(self):
        return {"type": "Category type"}

    @property
    def db(self):
        return self.resource_provider.db(self.path[1:])  # remove first slash

    def get_member_names(self):
        r = [f + e for f in self.db.table_names for e in self.formats]
        # print(f'get_member_names() -> {r}')
        return r

    def get_member(self, name):
        if name in self.get_member_names():
            return TableArtifact(
                path=join_uri(self.path, name), environ=self.environ, db_collection=self
            )
        return None


class _VirtualNonCollection(DAVNonCollection):
    """Abstract base class for all non-collection resources."""

    def __init__(self, path, environ):
        super().__init__(path, environ)

    def get_content_length(self):
        return None

    def get_content_type(self):
        return None

    def get_creation_date(self):
        return None

    def get_display_name(self):
        return self.name

    def get_display_info(self):
        raise NotImplementedError

    def get_etag(self):
        return None

    def support_etag(self):
        return False

    def get_last_modified(self):
        return None

    def support_ranges(self):
        return False


#    def handle_delete(self):
#        raise DAVError(HTTP_FORBIDDEN)
#    def handle_move(self, destPath):
#        raise DAVError(HTTP_FORBIDDEN)
#    def handle_copy(self, destPath, depthInfinity):
#        raise DAVError(HTTP_FORBIDDEN)


class TableArtifact(_VirtualNonCollection):
    """A virtual file, containing resource descriptions."""

    def __init__(self, path, environ, db_collection):
        #        assert name in _artifactNames
        super().__init__(path, environ)
        _, format = os.path.splitext(path)
        self.format = format
        self.db_collection = db_collection

    def get_content_length(self):
        return len(self.get_content().read())

    def get_content_type(self):
        _, ext = os.path.splitext(self.name)
        if ext == ".json":
            return "application/json"
        else:
            return "text/plain"

    def get_display_info(self):
        return {"type": "Virtual info file"}

    def prevent_locking(self):
        return True

    def get_ref_url(self):
        return quote(self.provider.share_path + self.name)

    def get_content(self):
        name, format = os.path.splitext(self.name)
        return self.db_collection.db.table_contents(name, format=format)


class DBResourceProvider(DAVProvider):
    """
    DAV provider that serves a VirtualResource derived structure.
    """

    def __init__(
        self, db_paths=[], formats=TABLE_FORMATTERS.keys(), allow_abspath=False
    ):
        self.formats = formats
        self.db_paths = db_paths
        self.allow_abspath = allow_abspath
        super().__init__()
        db_names = self.dbs
        assert (len(db_names)) == len(set(db_names)), "database names must be unique"

    def db(self, local_path):
        if "/" in local_path:
            return DB("/" + local_path)
        else:
            return {d.name: d for d in self.dbs}[local_path]

    @property
    def dbs(self):
        return [DB(p) for p in self.db_paths]

    def get_resource_inst(self, path, environ):
        # _logger.info("get_resource_inst('%s')" % path)
        self._count_get_resource_inst += 1
        root = RootCollection(environ, self, formats=self.formats)
        return root.resolve("", path)


class DB:
    def __init__(self, path):
        self.path = path

    @property
    def name(self):
        return os.path.basename(self.path)

    @property
    def con(self):
        return sqlite3.connect(self.path)

    @property
    def table_names(self):
        return [
            i[0]
            for i in self.con.cursor()
            .execute("SELECT name from sqlite_master where type ='table';")
            .fetchall()
        ]

    def table_contents(self, table_name, format=".csv"):
        assert table_name in self.table_names
        query = f"SELECT * from {table_name}"  # Required to avoid SQL injection
        o = io.BytesIO()
        TABLE_FORMATTERS[format](query, self.con, o)
        o.seek(0)
        return o
