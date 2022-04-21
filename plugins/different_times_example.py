from calendar import THURSDAY, weekday
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

from datetime import timedelta
from typing import Optional

from pendulum import Date, DateTime, Time, timezone

UTC = timezone("UTC")
MONDAY_TIMERANGE = (14, 21)
THURSDAY_TIMERANGE = (16,22)

class DifferentTimesTimetable(Timetable):

    # def infer_data_interval(self, run_after: DateTime) -> DataInterval:
    #     weekday = run_after.weekday()

    #     if weekday is 0:    #Monday -> last thursday
    #         delta = timedelta(days=4)
    #         time_range = THURSDAY_TIMERANGE
    #     elif weekday < 3: #tuesday, wednesday -> monday
    #         delta = timedelta(days=weekday)
    #     else: #thursday, friday, saturday, sunday -> thursday
    #         delta = timedelta(days=(weekday - 3) % 7)
    #         time_range = THURSDAY_TIMERANGE
    #     start = 
    #     end =

    def _compute_delta_time(weekday):
        if weekday is 0: # Monday -> Last Thursday
            delta = timedelta(days=4)
            time_range = THURSDAY_TIMERANGE # 4PM - 10PM
        elif weekday < 3: # Tuesday, Wednesday -> Monday
            delta = timedelta(days=weekday)
            time_range = MONDAY_TIMERANGE # 2PM - 9PM
        else: # Thursday, Friday, Saturday, Sunday -> Thursday
            delta = timedelta(days=(weekday - 3) % 7)
            time_range = THURSDAY_TIMERANGE # 4PM - 10PM
        return delta, time_range

    def infer_data_interval(self, run_after: DateTime) -> DataInterval:
        weekday = run_after.weekday()
        delta, time_range = self._compute_delta_time(weekday)
        start   = DateTime.combine((run_after - delta).date(), Time(hour=time_range[0])).replace(tzinfo=UTC)
        end     = DateTime.combine(start.date(), Time(hour=time_range[1])).replace(tzinfo=UTC)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(self, *, last_automated_data_interval: Optional[DataInterval],
restriction: TimeRestriction) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:
            last_start = last_automated_data_interval.start
            # If previous run was between Monday -> next Thursday
            if last_start.weekday() == 0:
                next_start = last_start + timedelta(days=3)
                time_range = THURSDAY_TIMERANGE
            # If last run was Thursday -> go to Monday
            else:
                next_start = last_start + timedelta(days=(7 - last_start.weekday()))
                time_range = MONDAY_TIMERANGE
                next_start = DateTime.combine(next_start.date(), Time(hour=time_range[0])).replace(tzinfo=UTC)
        else:
            next_start = restriction.earliest
            # No start_date or next date

        if next_start is None:
            return None

        # Catchup = False
        if not restriction.catchup:
            next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            delta, time_range = self._compute_delta_time(next_start.weekday())
            next_start = DateTime.combine((next_start.date() - delta).date(), Time(hour=time_range[0])).replace(tzinfo=UTC)
            next_end = DateTime.combine(next_start.date(), Time(hour=time_range[1])).replace(tzinfo=UTC)

        if restriction.latest is not None and next_start > restriction.latest:
            return None
        return DagRunInfo.interval(start=next_start, end=next_end)