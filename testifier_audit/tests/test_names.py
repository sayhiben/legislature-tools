from __future__ import annotations

import pandas as pd

from testifier_audit.config import NamesConfig
from testifier_audit.preprocess.names import add_name_features


def test_add_name_features_parses_last_first() -> None:
    df = pd.DataFrame({"name": ["Doe, Jane"]})
    cfg = NamesConfig(nickname_map_path="/tmp/does-not-exist.csv")
    out = add_name_features(df=df, config=cfg)

    assert out.loc[0, "last"] == "DOE"
    assert out.loc[0, "first"] == "JANE"
    assert out.loc[0, "first_initial"] == "J"
    assert out.loc[0, "canonical_name"] == "DOE|JANE"
