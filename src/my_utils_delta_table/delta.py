import logging
import json
import pyarrow as pa
import polars as pl
from deltalake import write_deltalake, DeltaTable


logger = logging.getLogger(__name__)


def check_if_deltatable(
    path: str,
    storage_options: dict = None
) -> bool:
    """
    Checks if a given location contains a Delta table in a delta lake.

    Parameters:
        path (str): Path of the Delta table in the Delta Lake.
        storage_options (dict, optional): Storage options (e.g., authentication credentials).
    
    Returns:
        bool: True if the location contains a Delta table, False otherwise.
    
    Raises:
        exception: If there is an error during the check.
    """
    logger.info(f"Checking if a Delta table exists at the specified path: {path}")

    try:
        # Check if the location is a Delta table
        is_deltatable = DeltaTable.is_deltatable(
            table_uri=path,
            storage_options=storage_options
        )
        # Log the result
        if is_deltatable:
            logger.info(f"The path '{path}' contains a valid Delta table.")
        else:
            logger.warning(f"The path '{path}' does not contain a Delta table or it does not exist.")
        return is_deltatable

    except Exception as e:
        logger.error(f"An error occurred while checking if the path '{path}' is a Delta table: {e}")
        raise



def write_table_to_deltalake(
    table,
    path: str,
    storage_options = None,
    partition_by = None,
    mode  = "overwrite"
):
    """
    Writes a table to a Delta Lake.

    Parameters:
        table (Union[pa.Table, pl.DataFrame]): The table to write, either as an Arrow Table or a Polars DataFrame.
        path (str): Path where the Delta table will be written.
        storage_options (dict, optional): Storage options (e.g., authentication credentials). Defaults to None.
        mode (str, optional): Write mode, e.g., "overwrite" or "append". Defaults to "overwrite".

    Raises:
        ValueError: If the input table is not a supported type.
        Exception: If there is an error during the write operation.
    """
    try:
        logger.info(f"Writing Delta table to: {path} with mode: {mode}")

        # Convert Polars DataFrame to Arrow Table if necessary
        if isinstance(table, pl.DataFrame):
            logger.debug("Converting Polars DataFrame to Arrow Table.")
            table = table.to_arrow()
        elif not isinstance(table, pa.Table):
            raise ValueError("Input table must be an Arrow Table or a Polars DataFrame.")

        # Write the table to Delta Lake
        write_deltalake(
            path,
            table,
            mode = mode,
            storage_options = storage_options,
            partition_by = partition_by
        )
        logger.info("Delta table write successful.")

    except ValueError as ve:
        logger.error(f"Invalid input table: {ve}")
        raise
    except Exception as e:
        logger.error(f"Error writing Delta table to path '{path}': {e}")
        raise


def read_table_in_deltalake(
    path: str,
    storage_options: dict = None,
    to_polars: bool = False
):
    """
    Reads a Delta Lake table into an Arrow Table or a Polars DataFrame.

    Parameters:
        path (str): Path of the Delta table in the Delta Lake.
        storage_options (dict, optional): Storage options (e.g., authentication credentials).
        to_polars (bool, optional): If True, converts the table to a Polars DataFrame. Defaults to False.

    Returns:
        Union[pa.Table, pl.DataFrame]: Arrow Table or Polars DataFrame containing the data from the Delta table.

    Raises:
        Exception: If there is an error while reading the Delta table.
    """
    logger.info(f"Reading Delta table from path: {path}")

    try:
        # Load the Delta table
        delta_table = DeltaTable(path, storage_options=storage_options)
        arrow_table = delta_table.to_pyarrow_table()

        # Convert to Polars DataFrame if requested
        if to_polars:
            result = pl.from_arrow(arrow_table)
            logger.info("Delta table successfully converted to Polars DataFrame.")
        else:
            result = arrow_table
            logger.info("Delta table successfully read as Arrow Table.")

        return result

    except Exception as e:
        logger.error(f"Error reading Delta table from path '{path}': {e}")
        raise


def read_deltalake_metadata(
    path: str,
    storage_options: dict = None
):
    """
    Reads metadata of a Delta Lake table.

    Parameters:
        path (str): Path of the Delta table in the Delta Lake.
        storage_options (dict, optional): Storage options (e.g., authentication credentials).

    Returns:
        dict: A dictionary containing the metadata, schema (in JSON format), and version of the Delta table.

    Raises:
        Exception: If there is an error while reading the Delta table metadata.
    """
    try:
        dt = DeltaTable(path, storage_options=storage_options)
        logger.info(f"Loaded Delta table at version {dt.version()}")
        return {
            "metadata": dt.metadata(),
            "schema": json.loads(dt.schema().to_json()),
            "version": dt.version()
        }
    except Exception as e:
        logger.error(f"Error reading Delta table metadata: {e}")
        raise


def upsert_delta_table(
    df_with_changes: pl.DataFrame,
    path: str,
    id_column: str,
    storage_options: dict = None
) -> None:
    """
    Upserts (merges) changes into an existing Delta table in the delta lake.¨

    Parameters:
        df_with_changes (pl.DataFrame): Polars DataFrame containing the changes to upsert.
        id_column (str): The column name used as the unique identifier for the merge operation.
        logger (logging.Logger): Logger for logging messages.
        storage_options (dict, optional): Storage options (e.g., authentication credentials).
    Raises:
        ValueError: If the id_column is not provided.
        Exception: If there is an error during the upsert operation.
    """
    # Log the action
    logger.info(f"Initiating upsert operation on Delta table located at path: '{path}' using '{id_column}' as the unique identifier.")

    try:
        # Perform the upsert (merge) operation
        (
            df_with_changes
            .write_delta(
                path,
                mode='merge',
                delta_merge_options={
                    'predicate': f'source.{id_column} = target.{id_column}',
                    'source_alias': 'source',
                    'target_alias': 'target',
                },
                storage_options=storage_options
            )
            .when_matched_update_all()  # Update matched rows
            .when_not_matched_insert_all()  # Insert unmatched rows
            .when_not_matched_by_source_delete()
            .execute()  # Execute the operation
        )
        logger.info(f"Upsert operation successfully completed for the Delta table at path: '{path}' using '{id_column}' as the unique identifier.")

    except Exception as e:
        logger.error(f"Upsert operation failed for Delta table at path '{path}' using '{id_column}' as the unique identifier: {str(e)}")
        raise
