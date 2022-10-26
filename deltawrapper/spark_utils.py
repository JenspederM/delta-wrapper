import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession


@dataclass
class FileInfoFixture:
    """
    This class mocks the DBUtils FileInfo object
    """

    path: str
    name: str
    size: int
    modificationTime: int


class DBUtilsSecrets:
    def __init__(self):
        self.secrets = self

    def get(self, scope, name):
        return os.environ.get(name)


class DBUtilsFixture:
    """
    This class is used for mocking the behaviour of DBUtils inside tests.
    """

    def __init__(self):
        self.fs = self
        self.secrets = DBUtilsSecrets()

    def cp(self, src: str, dest: str, recurse: bool = False):
        copy_func = shutil.copytree if recurse else shutil.copy
        copy_func(src, dest)

    def ls(self, path: str):
        _paths = Path(path).glob("*")
        _objects = [
            FileInfoFixture(
                str(p.absolute()), p.name, p.stat().st_size, int(p.stat().st_mtime)
            )
            for p in _paths
        ]
        return _objects

    def mkdirs(self, path: str):
        Path(path).mkdir(parents=True, exist_ok=True)

    def mv(self, src: str, dest: str, recurse: bool = False):
        copy_func = shutil.copytree if recurse else shutil.copy
        shutil.move(src, dest, copy_function=copy_func)

    def put(self, path: str, content: str, overwrite: bool = False):
        _f = Path(path)

        if _f.exists() and not overwrite:
            raise FileExistsError("File already exists")

        _f.write_text(content, encoding="utf-8")

    def rm(self, path: str, recurse: bool = False):
        deletion_func = shutil.rmtree if recurse else os.remove
        deletion_func(path)


def get_spark_session(spark: SparkSession = None):
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


def get_dbutils(
    spark: Optional[SparkSession] = None,
) -> Optional[Any]:  # pragma: no cover
    if spark is None:
        spark = get_spark_session()

    try:
        from pyspark.dbutils import DBUtils  # noqa

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return DBUtilsFixture()


def get_log4j_logger(spark: SparkSession, name: str) -> logging.Logger:
    # Avoid "INFO [py4j.java_gateway.run:2384] Received command c on object id p0"
    logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    logger = spark._jvm.org.apache.log4j
    spark.sparkContext.setLogLevel("INFO")
    return logger.LogManager.getLogger(name)
