"""Creates query code for your serving pipeline."""

import argparse
from typing import Type


class QueryGenerator:
    """A class to generate queries.

    Attributes:
        data_project_id: GCP project ID that contains BigQuery source data.
        other arguments:
    """

    def __init__(
            self,
            args: Type[argparse.Namespace],
            prediction_ts: str) -> None:
        """Creates a QueryGenerator object."""
        self.data_project_id = args.data_project_id
        self.prediction_ts = prediction_ts
        # other arguments


    def get_predictions_view(self) -> str:
        """Creates view pointing at the resulting recommendations.

        Returns:
            String query.
        """
        query = f"""
        CREATE OR REPLACE VIEW `your_dataset.target_view` AS
        SELECT *
        FROM `your_dataset.table_with_predictions`
        WHERE prediction_ts = PARSE_TIMESTAMP(
            "%Y-%m-%d %H:%M:%E*S", 
            "{self.prediction_ts}")
        """
        return query
