from databricks.sdk.runtime import dbutils


def username():
    """Return username, stripped for dots, part of users's email to use as dev db prefix"""
    email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    name = email.split("@")[0].replace(".", "")
    return name


def tblname(*, 
tbl,
db="nyc_workshop",
catalog="hive_metastore",
env="dev",
):
    if not tbl:
        raise ValueError("tbl must be a non-empty string")
    if not db:
        raise ValueError("db must be a non-empty string")
    db_prefix = ""
    if env == "dev":
        uname = username()
        db_prefix = f"dev_{uname}_"
    return f"{catalog}.{db_prefix}{db}.{tbl}"
    # ignore catalog for now, until UC is enabled
    # return f"{db_prefix}{db}.{tbl}"
    