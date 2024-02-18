import sqlite3
import pandas as pd
import pandas.testing
import io

from unittest import TestCase
from unittest.mock import patch, MagicMock


import wddbfs.main


table_contents = b"A,B,C\n1,2,3"
DBMock = MagicMock()
DBMock.name = "test.sqlite"
DBMock.table_names = ["test_table"]
DBMock.table_contents.return_value = table_contents

DBMockClass = MagicMock()
DBMockClass.return_value = DBMock
DBMockClass = DBMockClass


class TestDBResourceProvider(TestCase):
    @patch("wddbfs.main.DB", new=DBMockClass)
    def test_table_contents(self):
        rp = wddbfs.main.DBResourceProvider(db_paths=["test"])
        root = rp.get_resource_inst("/", MagicMock())
        self.assertEqual(root.get_member_names(), ("test.sqlite",))

        dbcollection = root.get_member("test.sqlite")
        self.assertEqual(
            dbcollection.get_member_names(),
            ["test_table.csv", "test_table.tsv", "test_table.json", "test_table.jsonl"],
        )

        table_artifact = dbcollection.get_member("test_table.csv")
        self.assertEqual(table_artifact.get_content(), table_contents)

# TODO: clean this up by handling mocks in setUp
expected = pd.DataFrame({"path": ["/test"], "content": [b"\x01\x02\x03"], "size": [99]})
ConnectionMock = MagicMock(spec=sqlite3.Connection)
ConnectionMock.execute.return_value.description = [[c] for c in expected.columns]
ConnectionMock.execute.return_value.fetchall.return_value = [expected.iloc[0].tolist()]


class TestDBResourceProviderFromSqlite(TestCase):
    @patch("wddbfs.main.DB.con", new=ConnectionMock)
    @patch(
        "wddbfs.main.DB.table_names", new=["test_table"]
    )  # So that we don't have to also return the table names in the sqlite connection mock
    def test_table_contents_with_blobs(self):
        rp = wddbfs.main.DBResourceProvider(db_paths=["test.sqlite"])
        root = rp.get_resource_inst("/", MagicMock())
        self.assertEqual(root.get_member_names(), ("test.sqlite",))

        dbcollection = root.get_member("test.sqlite")
        self.assertEqual(dbcollection.db.table_names, ["test_table"])

        table_artifact = dbcollection.get_member("test_table.jsonl")
        result = pd.read_json(io.BytesIO(table_artifact.get_content().read()), lines=True)
        pandas.testing.assert_frame_equal( result, expected)

        table_artifact = dbcollection.get_member("test_table.csv")
        result = pd.read_csv(io.BytesIO(table_artifact.get_content().read()))
        pandas.testing.assert_frame_equal( result, expected)


