import unittest

from src.label_reconciliations.fields.box_field import BoxField
from src.label_reconciliations.flag import Flag


class TestBoxField(unittest.TestCase):
    def test_reconcile_01(self):
        """It handles the happy case."""
        group = [
            BoxField(left=0, top=0, right=30, bottom=30),
            BoxField(left=10, top=10, right=40, bottom=40),
            BoxField(left=20, top=20, right=50, bottom=50),
        ]
        self.assertEqual(
            BoxField.reconcile(group, row_count=len(group)),
            BoxField(
                note="There are 3 box records",
                flag=Flag.OK,
                left=10,
                top=10,
                right=40,
                bottom=40,
            ),
        )
