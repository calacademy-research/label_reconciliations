import json
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from label_reconciliations.fields.base_field import BaseField

from label_reconciliations.flag import Flag
from label_reconciliations.utils import P
from label_reconciliations.utils import Point


@dataclass(kw_only=True)
class PolygonField(BaseField):
    points: list[Point] = field(default_factory=list)

    def to_dict(self, reconciled=False) -> dict[str, Any]:
        points = [{"x": int(round(p.x)), "y": int(round(p.y))} for p in self.points]
        field_dict = {self.header("points"): json.dumps(points)}
        return field_dict

    @classmethod
    def reconcile(cls, group, row_count, args=None):
        use = [g for g in group if g is not None]
        if not use:
            return None

        note = (
            f'There {P("is", len(use))} {len(use)} '
            f'of {row_count} polygon {P("record", row_count)}'
        )
        points = deepcopy(group[0].points)
        return cls.like(group, note=note, flag=Flag.OK, points=points)
