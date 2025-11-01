from src.transactions_pipeline import parse_rows, format_output, SchemaError
import datetime
import pytest


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
