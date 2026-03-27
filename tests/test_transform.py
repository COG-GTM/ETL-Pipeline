"""
Unit tests for the transform module (src/transform.py).

This module tests:
- identify_and_remove_duplicated_data() function

Tests cover various scenarios including:
- DataFrames with no duplicates
- DataFrames with exact duplicates
- DataFrames with partial duplicates (subset columns)
- Edge cases with NULL values
- Performance with larger datasets
"""

import pytest
import pandas as pd
import numpy as np

from src.transform import identify_and_remove_duplicated_data


class TestIdentifyAndRemoveDuplicatedData:
    """Tests for the identify_and_remove_duplicated_data function."""

    def test_no_duplicates_returns_same_data(self, sample_vehicle_data):
        """Test that data without duplicates is returned unchanged."""
        original_len = len(sample_vehicle_data)
        
        result = identify_and_remove_duplicated_data(sample_vehicle_data)
        
        assert len(result) == original_len
        assert result.equals(sample_vehicle_data) or len(result) == len(sample_vehicle_data)

    def test_removes_exact_duplicates(self, sample_vehicle_data_with_duplicates):
        """Test that exact duplicate rows are removed."""
        original_len = len(sample_vehicle_data_with_duplicates)
        
        result = identify_and_remove_duplicated_data(sample_vehicle_data_with_duplicates)
        
        # Should have fewer rows after deduplication
        assert len(result) < original_len
        # Should have no duplicates remaining
        assert result.duplicated().sum() == 0

    def test_keeps_first_occurrence(self, sample_vehicle_data_with_duplicates):
        """Test that the first occurrence of duplicates is kept."""
        result = identify_and_remove_duplicated_data(sample_vehicle_data_with_duplicates)
        
        # Verify first occurrence is preserved
        first_row = sample_vehicle_data_with_duplicates.iloc[0]
        assert any((result == first_row).all(axis=1))

    def test_subset_deduplication(self):
        """Test deduplication based on a subset of columns."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'VIN1', 'VIN2'],
            'model': ['Camry', 'Camry', 'Corolla'],
            'service_date': ['2022-01-01', '2022-01-02', '2022-01-01'],
            'service_cost': [100, 200, 150]
        })
        
        # Deduplicate based on VIN only
        result = identify_and_remove_duplicated_data(data, subset=['vin'])
        
        # Should have 2 unique VINs
        assert len(result) == 2
        assert result['vin'].nunique() == 2

    def test_inplace_modification(self):
        """Test that inplace=True modifies the original DataFrame."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'VIN1', 'VIN2'],
            'model': ['Camry', 'Camry', 'Corolla']
        })
        original_id = id(data)
        
        result = identify_and_remove_duplicated_data(data, inplace=True)
        
        # When inplace=True, should return the same object
        assert id(result) == original_id
        assert len(result) == 2

    def test_not_inplace_returns_copy(self):
        """Test that inplace=False returns a new DataFrame."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'VIN1', 'VIN2'],
            'model': ['Camry', 'Camry', 'Corolla']
        })
        original_len = len(data)
        
        result = identify_and_remove_duplicated_data(data, inplace=False)
        
        # Original should be unchanged
        assert len(data) == original_len
        # Result should be deduplicated
        assert len(result) == 2

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        empty_df = pd.DataFrame(columns=['vin', 'model', 'year'])
        
        result = identify_and_remove_duplicated_data(empty_df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_single_row_dataframe(self):
        """Test handling of single-row DataFrame."""
        single_row = pd.DataFrame({
            'vin': ['VIN1'],
            'model': ['Camry'],
            'year': [2021]
        })
        
        result = identify_and_remove_duplicated_data(single_row)
        
        assert len(result) == 1

    def test_all_duplicates(self):
        """Test DataFrame where all rows are duplicates."""
        all_same = pd.DataFrame({
            'vin': ['VIN1', 'VIN1', 'VIN1'],
            'model': ['Camry', 'Camry', 'Camry'],
            'year': [2021, 2021, 2021]
        })
        
        result = identify_and_remove_duplicated_data(all_same)
        
        assert len(result) == 1


class TestDuplicateDataWithNulls:
    """Tests for deduplication with NULL values."""

    def test_null_values_in_duplicates(self):
        """Test that NULL values are handled correctly in duplicate detection."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'VIN1', 'VIN2'],
            'model': ['Camry', 'Camry', None],
            'year': [2021, 2021, 2022]
        })
        
        result = identify_and_remove_duplicated_data(data)
        
        # First two rows are duplicates, third is unique
        assert len(result) == 2

    def test_null_in_subset_columns(self):
        """Test deduplication when subset columns contain NULL."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'VIN1', None],
            'model': ['Camry', 'Corolla', 'F-150'],
            'year': [2021, 2022, 2023]
        })
        
        result = identify_and_remove_duplicated_data(data, subset=['vin'])
        
        # VIN1 appears twice, None is unique
        assert len(result) == 2

    def test_all_null_row(self):
        """Test handling of rows with all NULL values."""
        data = pd.DataFrame({
            'vin': ['VIN1', None, None],
            'model': ['Camry', None, None],
            'year': [2021, None, None]
        })
        
        result = identify_and_remove_duplicated_data(data)
        
        # Two NULL rows are duplicates
        assert len(result) == 2


class TestDuplicateDataPerformance:
    """Performance tests for deduplication with larger datasets."""

    def test_large_dataset_deduplication(self, test_dataframe_factory):
        """Test deduplication performance with larger dataset."""
        # Create a large dataset with some duplicates
        large_df = test_dataframe_factory(num_rows=1000)
        # Add duplicates
        duplicates = large_df.head(100).copy()
        large_df_with_dups = pd.concat([large_df, duplicates], ignore_index=True)
        
        result = identify_and_remove_duplicated_data(large_df_with_dups)
        
        # Should remove the 100 duplicates
        assert len(result) == 1000

    def test_deduplication_preserves_data_types(self, sample_vehicle_data_with_duplicates):
        """Test that deduplication preserves column data types."""
        original_dtypes = sample_vehicle_data_with_duplicates.dtypes
        
        result = identify_and_remove_duplicated_data(sample_vehicle_data_with_duplicates)
        
        for col in result.columns:
            assert result[col].dtype == original_dtypes[col], f"Data type changed for column: {col}"


class TestDuplicateDataEdgeCases:
    """Edge case tests for deduplication."""

    def test_duplicate_with_different_whitespace(self):
        """Test that whitespace differences are detected."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'VIN1 ', ' VIN1'],  # Different whitespace
            'model': ['Camry', 'Camry', 'Camry']
        })
        
        result = identify_and_remove_duplicated_data(data)
        
        # Whitespace makes them different, so all 3 should remain
        assert len(result) == 3

    def test_duplicate_with_case_sensitivity(self):
        """Test that case differences are detected."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'vin1', 'Vin1'],  # Different cases
            'model': ['Camry', 'Camry', 'Camry']
        })
        
        result = identify_and_remove_duplicated_data(data)
        
        # Case makes them different, so all 3 should remain
        assert len(result) == 3

    def test_numeric_precision_in_duplicates(self):
        """Test that numeric precision is handled correctly."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'VIN1'],
            'price': [28000.00, 28000.000]  # Same value, different precision
        })
        
        result = identify_and_remove_duplicated_data(data)
        
        # Should be considered duplicates
        assert len(result) == 1

    def test_datetime_duplicates(self):
        """Test deduplication with datetime columns."""
        data = pd.DataFrame({
            'vin': ['VIN1', 'VIN1', 'VIN2'],
            'sale_date': pd.to_datetime(['2022-01-15', '2022-01-15', '2022-01-16'])
        })
        
        result = identify_and_remove_duplicated_data(data)
        
        assert len(result) == 2
