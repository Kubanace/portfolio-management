
from datetime import date


class Schedule(object):
    def __init__(self,
                 start_date: date = None,
                 end_date: date = None,
                 freq: int = None,
                 custom_dates_list: [date] = None):
        if custom_dates_list:
            sorted_dates_list = custom_dates_list.sort()
            self.dates_list = sorted_dates_list[0]
        else:
            self.dates_list = (end_date - start_date)/freq
        self.terms_list = [(dt - date.today()).years for dt in self.dates_list]
