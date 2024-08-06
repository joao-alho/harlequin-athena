from harlequin.options import (
    FlagOption,
    TextOption,
)

workgroup = TextOption(
    name="work_group",
    description="Athena workgroup to run queries on.",
    short_decls=["-w"],
)

s3_staging_dir = TextOption(
    name="s3_staging_dir",
    description="Athena staging dir.",
)

result_reuse_enable = FlagOption(
    name="result_reuse_enable",
    description=(
        "To enable result caching for the exact same query set this"
        + "flag to true and the option --result_reuse_minutes."
        + "Enabled by default."
    ),
    default=True,
)

unload = FlagOption(
    name="unload",
    description=(
        "Whether to use pyarrow unload to speed up queries"
        + "See Athena docs for the known limitations"
    ),
    default=False,
)


def _int_validator(s: str | None) -> tuple[bool, str]:
    if s is None:
        return True, ""
    try:
        _ = int(s)
    except ValueError:
        return False, f"Cannot convert {s} to an int!"
    else:
        return True, ""


result_reuse_minutes = TextOption(
    name="result_reuse_minutes",
    description=(
        "Set how long results of a query should be stored for."
        + "Default is 60 minutes."
    ),
    default="60",
    validator=_int_validator,
)

region_name = TextOption(
    name="region_name",
    description=(""),
)

aws_access_key_id = TextOption(
    name="aws_access_key_id",
    description="",
)

aws_secret_access_key = TextOption(
    name="aws_secret_access_key",
    description="",
)

aws_session_token = TextOption(
    name="aws_session_token",
    description="",
)

ATHENA_OPTIONS = [
    workgroup,
    s3_staging_dir,
    result_reuse_enable,
    result_reuse_minutes,
    unload,
    region_name,
    aws_access_key_id,
    aws_secret_access_key,
    aws_session_token,
]
