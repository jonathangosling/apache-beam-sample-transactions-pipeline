import argparse
import json
import apache_beam as beam
from datetime import datetime


class SchemaError(Exception):
    """Custom Exception to be raised on issues processing the ingested dataset."""

    pass


def parse_rows(row: str) -> dict:
    """
    Parses the PCollection rows of text.

    Splits the CSV string, and extracts and transforms timestamp and transaction_amount values.
    Timestamps converted to datetime objects, and transaction amounts converted to floats.
    :param: row: string representation of the CSV row.
    :return: transformed dictionary representation of the CSV row with headers added as keys.
    :raise: SchemaError: If the timestamp column or transaction_amount column are not as expected.
    """
    values = row.split(",")
    try:
        timestamp = datetime.strptime(values[0], "%Y-%m-%d %H:%M:%S UTC")
    except ValueError as e:
        raise SchemaError(
            "Issue processing timestamp in column 0. "
            f"Value: {values[0]}. Expected format %Y-%m-%d %H:%M:%S UTC. " + str(e)
        ) from e
    try:
        transaction_amount = float(values[3])
    except ValueError as e:
        raise SchemaError(
            "Issue processing transaction_amount in column 3. "
            f"Value: {values[3]}. Expected numeric type. " + str(e)
        ) from e

    return {"timestamp": timestamp, "transaction_amount": transaction_amount}


def format_output(row: dict) -> str:
    """
    Formats the post-transformation row into a JSON-type string.

    The total_amount field is converted to a string with two decimal places.
    :param: row: dictionary representation of the transformed row, with a "total_amount" key.
    :return: string json of the dictionary with the total_amount to 2 decimal places.
    """
    # Reformat total amount to string with precision 2
    row["total_amount"] = f"{row['total_amount']:.2f}"
    # Return as json string
    return json.dumps(row)


def transactions_pipleline(csv_file_path: str):
    """
    Executes an Apache Beam Pipeline to process the CSV file at `csv_file_path`.

    The transformed dataset is output as a compressed json file to "output/results.jsonl.gz".
    Transformations include:
    - Filtering out transactions with amount <= 20
    - Filtering out transactions made before 2010
    - Summing the transactions per date

    The expected CSV schema is:
    - Col 0: timestamp: datetime string of format YYYY-MM-DD HH:MM:SS UTC
    - Col 1: origin: string
    - Col 2: destination: string
    - Col 4: transaction_amount: numeric string

    The output JSON includes keys "date" and "total_amount".
    """

    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Read transactions"
            >> beam.io.ReadFromText(csv_file_path, skip_header_lines=True)
            | "Parse rows" >> beam.Map(parse_rows)
            | "Filter Transaction Amount"
            >> beam.Filter(lambda row: row["transaction_amount"] > 20)
            | "Filter Transaction Date"
            >> beam.Filter(lambda row: row["timestamp"].year >= 2010)
            | "Extract Date-Transaction key value pairs"
            >> beam.Map(
                lambda x: (x["timestamp"].strftime("%Y-%m-%d"), x["transaction_amount"])
            )
            | "Group by timestamp" >> beam.CombinePerKey(sum)
            | "Relabel"
            >> beam.Map(lambda row: {"date": row[0], "total_amount": row[1]})
            | "Format output" >> beam.Map(format_output)
            | "WriteTransactions"
            >> beam.io.WriteToText(
                "output/results.jsonl.gz", num_shards=1, shard_name_template=""
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv-file-path",
        dest="csv_file_path",
        help="File path of the transactions CSV to process",
        default="gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",
    )
    args = parser.parse_args()

    transactions_pipleline(csv_file_path=args.csv_file_path)
