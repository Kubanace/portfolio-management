
from abc import ABC, abstractmethod
from datetime import date
from typing import Union, List

from pylib.library.config.enumerations import DayCountConvention


class DayCounter(ABC):
    """Abstract base class for day count calculations"""

    @abstractmethod
    def year_fraction(self, start_date: date, end_date: date) -> float:
        """Calculate the year fraction between two dates"""
        pass

    @abstractmethod
    def day_count(self, start_date: date, end_date: date) -> int:
        """Calculate the number of days between two dates"""
        pass


class Thirty360(DayCounter):
    """Implementation of 30/360 day count convention"""

    @staticmethod
    def _thirty_360_components(start_date: date, end_date: date) -> int:
        """Calculate 30/360 day count components"""
        d1 = min(30, start_date.day)
        d2 = min(30, end_date.day) if d1 == 30 else end_date.day

        y1, m1 = start_date.year, start_date.month
        y2, m2 = end_date.year, end_date.month

        return 360 * (y2 - y1) + 30 * (m2 - m1) + (d2 - d1)

    def year_fraction(self, start_date: date, end_date: date) -> float:
        """
        Calculate the year fraction between two dates using 30/360 convention

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            float: Year fraction
        """
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")

        day_count = self._thirty_360_components(start_date, end_date)
        return day_count / 360

    def day_count(self, start_date: date, end_date: date) -> int:
        """
        Calculate the number of days between two dates using 30/360 convention

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            int: Number of days
        """
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")

        return self._thirty_360_components(start_date, end_date)


class ActualActual(DayCounter):
    """Implementation of Actual/Actual day count convention"""

    def year_fraction(self, start_date: date, end_date: date) -> float:
        """
        Calculate the year fraction between two dates using Actual/Actual convention

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            float: Year fraction
        """
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")

        if start_date.year == end_date.year:
            year_days = 366 if self._is_leap_year(start_date.year) else 365
            return self.day_count(start_date, end_date) / year_days

        # Handle multi-year periods
        fraction = 0.0

        # Handle first (partial) year
        year_end = date(start_date.year, 12, 31)
        year_days = 366 if self._is_leap_year(start_date.year) else 365
        fraction += self.day_count(start_date, year_end) / year_days

        # Handle middle years
        for year in range(start_date.year + 1, end_date.year):
            fraction += 1.0

        # Handle last (partial) year
        if end_date.year > start_date.year:
            year_start = date(end_date.year, 1, 1)
            year_days = 366 if self._is_leap_year(end_date.year) else 365
            fraction += self.day_count(year_start, end_date) / year_days

        return fraction

    def day_count(self, start_date: date, end_date: date) -> int:
        """
        Calculate the number of days between two dates using actual calendar days

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            int: Number of days
        """
        if start_date > end_date:
            raise ValueError("Start date must be before or equal to end date")

        return (end_date - start_date).days

    @staticmethod
    def _is_leap_year(year: int) -> bool:
        """Check if a year is a leap year"""
        return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


class DayCountCalculator:
    """Factory class for creating day count calculators"""

    _calculators = {
        DayCountConvention.THIRTY_360: Thirty360(),
        DayCountConvention.ACTUAL_ACTUAL: ActualActual(),
    }

    # _convention_mapping = {
    #     "30/360": DayCountConvention.THIRTY_360,
    #     "Actual/Actual": DayCountConvention.ACTUAL_ACTUAL,
    # }
    #
    # @classmethod
    # def get_supported_conventions(cls) -> List[str]:
    #     """
    #     Get list of supported convention string descriptions
    #
    #     Returns:
    #         List[str]: List of supported convention descriptions
    #     """
    #     return list(cls._convention_mapping.keys())

    # @classmethod
    # def _get_convention(cls, convention: Union[str, DayCountConvention]) -> DayCountConvention:
    #     """
    #     Convert string convention description to DayCountConvention enum
    #
    #     Args:
    #         convention: String description or DayCountConvention enum
    #
    #     Returns:
    #         DayCountConvention: Corresponding enum value
    #
    #     Raises:
    #         ValueError: If convention string is not recognized
    #     """
    #     if isinstance(convention, DayCountConvention):
    #         return convention
    #
    #     if isinstance(convention, str):
    #         # Normalize string by removing whitespace and converting to title case
    #         normalized = convention.strip().title()
    #         if convention_enum := cls._convention_mapping.get(normalized):
    #             return convention_enum
    #
    #         raise ValueError(f"Unrecognized day count convention: {convention}. "
    #                          f"Supported conventions: {list(cls._convention_mapping.keys())}")
    #
    #     raise ValueError(f"Convention must be string or DayCountConvention, got {type(convention)}")

    @classmethod
    def get_calculator(cls, convention: DayCountConvention) -> DayCounter:
        """
        Get the appropriate day count calculator for a given convention

        Args:
            convention: Day count convention to use

        Returns:
            DayCounter: Calculator instance for the specified convention

        Raises:
            ValueError: If convention is not supported
        """
        calculator = cls._calculators.get(convention)
        if calculator is None:
            raise ValueError(f"Unsupported day count convention: {convention}")
        return calculator

    @classmethod
    def calculate_year_fraction(cls,
                                start_date: date,
                                end_date: date,
                                convention: DayCountConvention) -> float:
        """
        Calculate year fraction between two dates using specified convention

        Args:
            start_date: Start date
            end_date: End date
            convention: Day count convention to use

        Returns:
            float: Year fraction
        """
        calculator = cls.get_calculator(convention)
        return calculator.year_fraction(start_date, end_date)

    @classmethod
    def calculate_day_count(cls,
                            start_date: date,
                            end_date: date,
                            convention: DayCountConvention) -> int:
        """
        Calculate number of days between two dates using specified convention

        Args:
            start_date: Start date
            end_date: End date
            convention: Day count convention to use

        Returns:
            int: Number of days
        """
        calculator = cls.get_calculator(convention)
        return calculator.day_count(start_date, end_date)



