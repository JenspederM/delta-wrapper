from pyspark.sql import SparkSession, DataFrame
from deltawrapper.spark_utils import get_log4j_logger

from typing import Dict, List, Union, Optional
import yaml
from pydantic import BaseModel


class Column:
    def __init__(self, name: str, aliases: Optional[List[str]] = []) -> None:
        """Create a new Column

        Args:
            name (str): The name of the column
            aliases (List[str], optional): The aliases of the column. Defaults to [].

        Raises:
            ValueError: If the name is already in the aliases
        """
        if name in aliases:
            raise ValueError(f"Name {name} already in aliases")
        else:
            self.logger = get_log4j_logger(
                SparkSession.builder.getOrCreate(), self.__class__.__name__
            )
            self.name = name
            self._aliases = aliases

    @property
    def aliases(self) -> List[str]:
        """Get the aliases of the column

        Returns:
            List[str]: The aliases of the column
        """
        return self._aliases

    def __repr__(self) -> str:
        return f"Output(name={self.name}, columns={self.columns})"

    def __str__(self) -> str:
        return self.__repr__()

    def add_alias(self, alias: str) -> None:
        """Add an alias to the column

        Args:
            alias (str): The alias to add

        Raises:
            ValueError: If the alias is already in the aliases
        """
        if alias in self._aliases:
            raise ValueError(f"Alias {alias} already in aliases")
        else:
            self.logger.info(f"Adding alias {alias} to column {self.name}")
            self._aliases.append(alias)

    def remove_alias(self, alias: str) -> None:
        """Remove an alias from the column

        Args:
            alias (str): The alias to remove

        Raises:
            ValueError: If the alias is not in the aliases
        """
        if alias not in self._aliases:
            raise ValueError(f"Alias {alias} not in aliases")
        else:
            self.logger.info(f"Removing alias {alias} from column {self.name}")
            self._aliases.remove(alias)

    def rename_from_alias(self, df: DataFrame, force: bool = False) -> DataFrame:
        """Rename a column from an alias

        Args:
            df (DataFrame): The DataFrame to rename the column in
            force (bool, optional): Whether to force the rename. Defaults to False.

        Returns:
            DataFrame: The DataFrame with the renamed column

        Raises:
            ValueError: If the column is not found in the DataFrame
            ValueError: If the column is found multiple times in the DataFrame and force is False
        """
        self.logger.info(f"Renaming column {self.name} from aliases {self.aliases}")
        cols = df.columns
        aliases_in_cols = [col for col in cols if col in self._aliases]

        if len(aliases_in_cols) > 1 and force is False:
            self.logger.error("Found multiple aliases in DataFrame")
            raise ValueError(f"Multiple aliases found in columns: {aliases_in_cols}")
        elif len(aliases_in_cols) == 0:
            self.logger.error("Alias not found in DataFrame")
            raise ValueError(f"No alias in columns: {aliases_in_cols}")

        for alias in aliases_in_cols:
            if force is True and len(aliases_in_cols) > 1:
                new_name = f"{self.name}__{alias}"
            else:
                new_name = self.name

            self.logger.info(f"Renaming column {alias} to {new_name}")
            df = df.withColumnRenamed(alias, new_name)

        return df


class Input(BaseModel):
    """Input configuration

    Args:
        path (str): The path to the input data
        ext (str): The extension of the input data

    Raises:
        ValueError: If the path is not a string
        ValueError: If the ext is not a string

    Returns:
        Input: The Input configuration"""

    path: str
    ext: str


class Output(BaseModel):
    """Output configuration

    Args:
        path (str): The path to the output data
        ext (str): The extension of the output data
        format (Dict[str, List[str]]): how to format the output data.
            The key is the name of the column, the value is a list of aliases.

    Raises:
        ValueError: If the path is not a string
        ValueError: If the ext is not a string
        ValueError: If the format is not a dict

    Returns:
        Output: The Output configuration
    """

    path: str
    format: Dict[str, Union[str, List[str]]]

    def apply_format(self, df):
        df_out = df
        output_df = [Column(name, aliases) for name, aliases in self.format.items()]
        for col in output_df:
            df_out = col.rename_from_alias(df_out, force=True)
        return df_out


class Dataset(BaseModel):
    """Dataset configuration

    Args:
        input (Input): The input configuration
        output (Output): The output configuration

    Raises:
        ValueError: If the input is not an Input
        ValueError: If the output is not an Output

    Returns:
        Dataset: The Dataset configuration
    """

    input: Input
    output: Output


class Config(BaseModel):
    """Configuration

    Args:
        datasets (List[Dataset]): The list of datasets

    Raises:
        ValueError: If the datasets is not a list

    Returns:
        Config: The configuration
    """

    datasets: List[Dataset]

    @classmethod
    def from_yaml(cls, path):
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return Config(**data)


if __name__ == "__main__":
    output_cols = {"out-2": ["in-c"], "out-1": ["in-a", "in-b"], "out-3": ["in-d"]}
    output_df = [Column(name, aliases) for name, aliases in output_cols.items()]
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.createDataFrame(
        [
            (1, 2, 3, 4),
            (5, 6, 7, 8),
            (9, 10, 11, 12),
        ],
        ["in-a", "in-b", "in-c", "in-d"],
    )

    for col in output_df:
        df = col.rename_from_alias(df, force=True)

    df.show()
