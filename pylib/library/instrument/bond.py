
from datetime import date
from typing import Union

from pylib.library.instrument.instrument import Instrument
from pylib.library.tsir.tsir import Tsir
from abc import ABC

from pylib.library.config.enumerations import DayCountConvention
import pylib.library.calendar.day_count as dc


class Bond(Instrument):
    def __init__(self,
                 #instrument_id: int,
                 coupon_rate: float,
                 maturity_date: date,
                 issuance_date: date,
                 face_value: int,
                 coupon_freq: float = 0.5,
                 convention: DayCountConvention = DayCountConvention.ACTUAL_ACTUAL):
        #Instrument.__init__(self, instrument_id)
        self.accrual_date = None
        self.next_coupon_date = None
        self.cashflows = None
        self.coupon_rate = coupon_rate
        self.coupon_freq = coupon_freq
        self.face_value = face_value
        self.maturity_date = maturity_date
        self.issuance_date = issuance_date

        self.convention = convention
        self.day_count_calculator = dc.DayCountCalculator().get_calculator(self.convention)
        self.term = self.day_count_calculator.day_count(start_date=self.issuance_date, end_date=self.maturity_date)
        # self.term = (self.maturity_date - self.issuance_date)

    def present_value(self, tsir: Tsir) -> float:
        _ir = tsir.interest_rates_list
        _t = tsir.terms_list
        _n_terms = len(_ir)
        _coupon_pv = [self.coupon_rate * self.face_value/((1 + _ir[i]) ^ (_t[i])) for i in range(_n_terms)]
        _principal_pv = self.face_value / ((1 + _ir[_n_terms-1]) ^ (_t[_n_terms-1]))
        return sum(_coupon_pv) + _principal_pv

    def yield_to_maturity(self):
        _ytm = 0
        return _ytm
