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
