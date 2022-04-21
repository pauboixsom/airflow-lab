from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

from datetime import timedelta
from typing import Optional

from pendulum import Date, DateTime, Time, timezone

UTC = timezone("UTC")
USUAL_OPEN_MARKET_HOUR = Time(14, 30, 0)
USUAL_CLOSE_MARKET_HOUR = Time(21, 0, 0)
EARLY_CLOSE_MARKET_HOUR = Time(17, 0, 0)
USUAL_DURATION_MARKET = timedelta(hours=6, minutes=30)
EARLY_CLOSE_DURATION_MARKET = timedelta(hours=2, minutes=30)

class TradingDayTimetable(Timetable):

    @property
    def special_days(self) -> dict[str, Time]:
        return {
            "2021-01-01": None, #New years day
            "2021--01-04": None,
            "2021--01-18": None,
            "2021--01-15": None,
            "2021--01-02": None,
            "2021--01-31": None,
            "2021--01-04": None,
            "2021--01-05": None,
            "2021--01-06": None,
            "2021-11-25": None,
            "2021-11-26": EARLY_CLOSE_MARKET_HOUR,
            "2021-12-24": None,
        }

    @property
    def summary(self) -> str:
        return f"different times"

    def infer_data_interval(self, run_after: DateTime) -> DataInterval:
        pass

    def next_dagrun_info(self, *,
                        last_automated_data_interval: Optional[DataInterval],
                        restriction: TimeRestriction) -> Optional[DagRunInfo]:

        next_start = self.compute_next_dagrun(last_automated_data_interval, restriction.earliest )

        duration= USUAL_DURATION_MARKET
        while next_start is not None and str(next_start.date()) in self.special_days:
            early_close = self.special_days[str(next_start.date())]
            if early_close is not None:
                duration = EARLY_CLOSE_DURATION_MARKET
                break
            next_start += timedelta(days=1)
            if last_automated_data_interval is not None:
                last_automated_data_interval.start = next_start
            next_start = self.compute_next_dagrun(last_automated_data_interval, next_start, )

        if restriction.latest is not None and next_start > restriction.latest:
            return None
        return DagRunInfo.interval(start=next_start, end=(next_start + duration))

    def compute_next_dagrun(self, last_automated_data_interval, next_start, restriction):

        if last_automated_data_interval is not None: