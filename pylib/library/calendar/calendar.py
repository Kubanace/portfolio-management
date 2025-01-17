

class Calendar:
    def __init__(self,
                 # calendar_type: str = "business_day",
                 # exchange: str = None
                 calendar_id: str):
        """

        :param calendar_type: type of the calendar object (calendar_day or business_day
        """
        self.calendar_id = calendar_id
        self.type = None
        self.exchange = None

    def load_calendar_metadata(self):
        query = ""
        # load query

    def get_interval_days(self, start_dt, end_dt):
        days_list = []
        return