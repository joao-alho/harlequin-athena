import sys

from harlequin import HarlequinCompletion
from harlequin_athena.completions import load_completions

if sys.version_info < (3, 10):
    pass
else:
    pass


def test_completions_parse() -> None:
    completions = load_completions()
    assert isinstance(completions[0], HarlequinCompletion)
    assert len(completions) > 1
