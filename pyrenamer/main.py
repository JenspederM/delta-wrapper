from pyspark.sql import SparkSession, DataFrame


class Column:
    def __init__(self, name, aliases):
        self.name = name
        self._aliases = aliases

    @property
    def aliases(self):
        return self._aliases

    def __repr__(self):
        return f"Output(name={self.name}, columns={self.columns})"

    def __str__(self):
        return self.__repr__()

    def add_alias(self, alias: str):
        self.aliases.append(alias)

    def remove_alias(self, alias: str):
        self.aliases.remove(alias)

    def rename_from_alias(self, df: DataFrame, force: bool = False):
        """Rename a column from an alias

        Args:
            df (DataFrame): The DataFrame to rename the column in
            force (bool, optional): Whether to force the rename. Defaults to False.

        Returns:
            DataFrame: The DataFrame with the renamed column

        Raises:
            ValueError: If the column is not found in the DataFrame
            ValueError: If the column is found multiple timesin the DataFrame and force is False
        """
        cols = df.columns
        alias_in_cols = [col for col in cols if col in self.aliases]

        if len(alias_in_cols) > 1:
            if force is False:
                raise ValueError(f"Multiple aliases found in columns: {alias_in_cols}")
            else:
                for col in alias_in_cols:
                    df = df.withColumnRenamed(col, f"{self.name}_{col}")
        elif len(alias_in_cols) == 0:
            raise ValueError(f"No alias in columns: {alias_in_cols}")
        else:
            alias = alias_in_cols[0]
            df = df.withColumnRenamed(alias, self.name)

        return df


if __name__ == "__main__":
    output_cols = {"out-1": ["in-a", "in-b"], "out-2": ["in-c"], "out-3": ["in-d"]}
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
