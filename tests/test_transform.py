import pandas as pd
import pytest

from src.transform import identify_and_remove_duplicated_data


class TestIdentifyAndRemoveDuplicatedData:
    """Tests for the identify_and_remove_duplicated_data function."""

    def test_removes_duplicates_default(self):
        """Should remove duplicate rows and return a new DataFrame."""
        df = pd.DataFrame({
            "a": [1, 2, 2, 3],
            "b": ["x", "y", "y", "z"],
        })
        result = identify_and_remove_duplicated_data(df)
        assert len(result) == 3
        assert list(result["a"]) == [1, 2, 3]

    def test_no_duplicates(self):
        """Should return a copy when there are no duplicates."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = identify_and_remove_duplicated_data(df)
        assert len(result) == 3
        # Should be a copy, not the same object
        assert result is not df

    def test_all_duplicates(self):
        """Should keep one row when all rows are duplicates."""
        df = pd.DataFrame({"a": [1, 1, 1], "b": ["x", "x", "x"]})
        result = identify_and_remove_duplicated_data(df)
        assert len(result) == 1

    def test_subset_deduplication(self):
        """Should deduplicate only on the specified subset of columns."""
        df = pd.DataFrame({
            "a": [1, 1, 2],
            "b": ["x", "y", "z"],
        })
        result = identify_and_remove_duplicated_data(df, subset=["a"])
        assert len(result) == 2

    def test_inplace_true(self):
        """Should modify the original DataFrame when inplace=True."""
        df = pd.DataFrame({
            "a": [1, 2, 2, 3],
            "b": ["x", "y", "y", "z"],
        })
        result = identify_and_remove_duplicated_data(df, inplace=True)
        assert len(result) == 3
        assert result is df

    def test_inplace_false_does_not_modify_original(self):
        """Should not modify the original DataFrame when inplace=False."""
        df = pd.DataFrame({
            "a": [1, 2, 2, 3],
            "b": ["x", "y", "y", "z"],
        })
        original_len = len(df)
        result = identify_and_remove_duplicated_data(df, inplace=False)
        assert len(df) == original_len
        assert len(result) == 3

    def test_keeps_first_occurrence(self):
        """Should keep the first occurrence of duplicates."""
        df = pd.DataFrame({
            "a": [1, 1],
            "b": ["first", "second"],
        })
        result = identify_and_remove_duplicated_data(df, subset=["a"])
        assert result.iloc[0]["b"] == "first"

    def test_empty_dataframe(self):
        """Should handle empty DataFrame gracefully."""
        df = pd.DataFrame({"a": [], "b": []})
        result = identify_and_remove_duplicated_data(df)
        assert len(result) == 0

    def test_single_row(self):
        """Should handle single-row DataFrame."""
        df = pd.DataFrame({"a": [1], "b": ["x"]})
        result = identify_and_remove_duplicated_data(df)
        assert len(result) == 1

    def test_preserves_columns(self):
        """Should preserve all columns in the output."""
        df = pd.DataFrame({
            "a": [1, 2, 2],
            "b": ["x", "y", "y"],
            "c": [10.0, 20.0, 20.0],
        })
        result = identify_and_remove_duplicated_data(df)
        assert list(result.columns) == ["a", "b", "c"]

    def test_no_duplicates_inplace(self):
        """Should return the original df when inplace=True and no duplicates."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = identify_and_remove_duplicated_data(df, inplace=True)
        assert result is df

    def test_with_nan_values(self):
        """Should handle NaN values in deduplication."""
        df = pd.DataFrame({
            "a": [1, 1, None, None],
            "b": ["x", "x", "y", "y"],
        })
        result = identify_and_remove_duplicated_data(df)
        # pandas considers two NaN as duplicates by default
        assert len(result) == 2
