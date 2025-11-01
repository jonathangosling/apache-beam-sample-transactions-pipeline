from src.transactions_pipeline import (
    TransformTransactions,
    parse_rows,
    format_output,
    SchemaError,
)
import datetime
import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

############################################################
# Unit tests for composite transform TransformTransactions #
############################################################


@pytest.mark.parametrize(
    "input_strings, output_strings",
    [
        (  # Test Case: single row, date and amount just in threshold
            ["2010-01-01 00:00:00 UTC,test_wallet_1,test_wallet_2,20.01"],
            ['{"date": "2010-01-01", "total_amount": "20.01"}'],
        ),
        (  # Test Case: no row within date threshold, just under
            ["2009-12-31 23:59:59 UTC,test_wallet_1,test_wallet_2,20.01"],
            [],
        ),
        (  # Test Case: no row within transaction amount threshold, just under
            ["2010-01-01 00:00:00 UTC,test_wallet_1,test_wallet_2,20.00"],
            [],
        ),
        (  # Test Case: negative transaction amount filtered out
            ["2010-01-01 00:00:00 UTC,test_wallet_1,test_wallet_2,-25"],
            [],
        ),
        (  # Test Case: no filtered transactions with the same date
            [
                "2009-12-31 23:59:59 UTC,test_wallet_1,test_wallet_2,20.00",
                "2009-12-31 23:59:59 UTC,test_wallet_1,test_wallet_2,2.00",
                "2011-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,2.00",
                "2011-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,21.30",
                "1999-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,21.30",
                "2020-01-29 17:30:45 UTC,test_wallet_1,test_wallet_2,100.99",
            ],
            [
                '{"date": "2011-02-12", "total_amount": "21.30"}',
                '{"date": "2020-01-29", "total_amount": "100.99"}',
            ],
        ),
        (  # Test Case: 2 filtered transactions with the same date
            [
                "2009-12-31 23:59:59 UTC,test_wallet_1,test_wallet_2,2.00",
                "2011-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,2.00",
                "2011-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,21.30",
                "1999-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,21.30",
                "2020-01-29 17:30:45 UTC,test_wallet_1,test_wallet_2,100.99",
                "2020-01-29 00:30:50 UTC,test_wallet_x,test_wallet_y,20.50",
                "2021-01-29 00:30:50 UTC,test_wallet_1,test_wallet_2,10000.00",
            ],
            [
                '{"date": "2011-02-12", "total_amount": "21.30"}',
                '{"date": "2020-01-29", "total_amount": "121.49"}',
                '{"date": "2021-01-29", "total_amount": "10000.00"}',
            ],
        ),
        (  # Test Case: multiple filtered transactions with the same dates
            [
                "2021-01-29 17:30:50 UTC,test_wallet_1,test_wallet_2,25",
                "2009-12-31 23:59:59 UTC,test_wallet_1,test_wallet_2,2.00",
                "2011-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,2.00",
                "2021-01-29 00:30:50 UTC,test_wallet_1,test_wallet_2,50.01",
                "2011-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,21.30",
                "1999-02-12 10:30:45 UTC,test_wallet_1,test_wallet_2,21.30",
                "2020-01-29 17:30:45 UTC,test_wallet_1,test_wallet_2,100.99",
                "2020-01-29 00:30:50 UTC,test_wallet_x,test_wallet_y,20.50",
                "2021-01-29 00:30:50 UTC,test_wallet_1,test_wallet_2,10000.00",
            ],
            [
                '{"date": "2011-02-12", "total_amount": "21.30"}',
                '{"date": "2020-01-29", "total_amount": "121.49"}',
                '{"date": "2021-01-29", "total_amount": "10075.01"}',
            ],
        ),
        (  # Test Case: transaction amounts to varying decimal places
            [
                "2021-01-29 17:30:50 UTC,test_wallet_1,test_wallet_2,25.1",
                "2020-01-29 17:30:45 UTC,test_wallet_1,test_wallet_2,100.00010",
            ],
            [
                '{"date": "2021-01-29", "total_amount": "25.10"}',
                '{"date": "2020-01-29", "total_amount": "100.00"}',
            ],
        ),
    ],
)
def test_transform_transactions(input_strings, output_strings):
    """
    Tests that TransformTransactions correctly processes the input CSV strings.
    Including:
        Filtering or dates and transaction amounts
        Grouping and aggregating transaction amounts by date
    """
    with beam.Pipeline() as p:
        result = p | beam.Create(input_strings) | TransformTransactions()
        assert_that(result, equal_to(output_strings))


#############################################
# Unit tests for CSV row parser: parse_rows #
#############################################


class TestParseRows:
    """Wrapper Class for unit tests of parse_rows"""

    @pytest.mark.parametrize(
        "row, expected_result",
        [
            (
                "2020-01-02 12:00:30 UTC,test_wallet_1,test_wallet_2,0",
                {
                    "timestamp": datetime.datetime(2020, 1, 2, 12, 00, 30),
                    "transaction_amount": 0,
                },
            ),
            (
                "2020-12-30 14:30:00 UTC,test_wallet_1,test_wallet_2,11.30",
                {
                    "timestamp": datetime.datetime(2020, 12, 30, 14, 30, 0),
                    "transaction_amount": 11.3,
                },
            ),
            (
                "2018-6-1 1:2:4 UTC,test_wallet_1,test_wallet_2,100.45",
                {
                    "timestamp": datetime.datetime(2018, 6, 1, 1, 2, 4),
                    "transaction_amount": 100.45,
                },
            ),
        ],
    )
    def test_parse_rows(self, row, expected_result):
        """
        Tests that parse_rows successfully returns a dictionary containing the timestamp and transaction_amount,
        from the correct columns and in the correct format.
        """
        assert parse_rows(row) == expected_result

    @pytest.mark.parametrize(
        "row,date_string",
        [
            (
                # Month too large
                "2020-30-12 12:00:30 UTC,test_wallet_1,test_wallet_2,0",
                "2020-30-12 12:00:30 UTC",
            ),
            (
                # Missing time
                "2020-01-12 UTC,test_wallet_1,test_wallet_2,0",
                "2020-01-12 UTC",
            ),
            (
                # Missing timezone
                "2020-01-12 12:00:30,test_wallet_1,test_wallet_2,0",
                "2020-01-12 12:00:30",
            ),
        ],
    )
    def test_parse_rows_timestamp_error(self, row, date_string):
        """
        Tests that parse_rows raise SchemaException if there's an issue with the timestamp.
        """
        with pytest.raises(
            SchemaError,
            match=(
                "Issue processing timestamp in column 0. "
                f"Value: {date_string}. Expected format %Y-%m-%d %H:%M:%S UTC. "
            ),
        ):
            parse_rows(row)

    def test_parse_rows_transaction_amount_error(self):
        """
        Tests that parse_rows raise SchemaException if there's an issue with the transaction_amount.
        """
        row = "2020-01-12 12:00:30 UTC,test_wallet_1,test_wallet_2,abc123"
        with pytest.raises(
            SchemaError,
            match=(
                "Issue processing transaction_amount in column 3. "
                "Value: abc123. Expected numeric type. "
            ),
        ):
            parse_rows(row)


#######################################################
# Unit tests for output JSON formatter: format_output #
#######################################################


@pytest.mark.parametrize(
    "row, expected_result",
    [
        (
            {"date": "2021-01-01", "total_amount": 0},
            '{"date": "2021-01-01", "total_amount": "0.00"}',
        ),
        (
            {"date": "2021-01-01", "total_amount": 11.011},
            '{"date": "2021-01-01", "total_amount": "11.01"}',
        ),
        (
            {"date": "2021-01-01", "total_amount": 5.10},
            '{"date": "2021-01-01", "total_amount": "5.10"}',
        ),
    ],
)
def test_format_output(row, expected_result):
    """
    Tests that format_output correctly converts the dictionary to JSON string with expected schema.
    """
    assert format_output(row) == expected_result
