from datetime import timedelta
from time import time
from typing import Optional

from pendulum import Date, DateTime, Time, timezone

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

UTC = timezone("UTC")


class WeekendsTimetable(Timetable):

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval: #al tuto i al videu està malament però ho arregla el mr taiwan https://github.com/apache/airflow/pull/19735
        weekday = run_after.weekday()
        if weekday in (0, 6):  # Monday and Sunday -- interval is last Friday. Aqui no faltari el saturday?
            days_since_friday = (run_after.weekday() - 4) % 7
            delta = timedelta(days=days_since_friday)
        elif weekday ==1 : #interval de dissabte, si es fa el trigger dimarts
            delta = timedelta(days=3)
            end = timedelta(days=3)
        else:  # Otherwise the interval is yesterday.
            delta = timedelta(days=1)
            end = timedelta(days=1)
        start = DateTime.combine((run_after - delta).date(), Time.min).replace(tzinfo=UTC)
        return DataInterval(start=start, end=(start + end))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            last_start_weekday = last_start.weekday()
            if 1 <= last_start_weekday < 5:  # Last run on Tuesday through Friday -- next is tomorrow.
                delta = timedelta(days=1)
                if last_start_weekday == 4: #si l'ultim es divendres el següent és dissabte i els dissabtes són de 3 dies
                    next_end=timedelta(days=3)
                else:
                    next_end=timedelta(days=1) #la resta 1
            else:  # Last run on Saturday -- skip to next Tuesday. #
                delta = timedelta(days= 3) #dela de 7-4 = 3 dies hauria de ser 1 perque despres del friday de 0 a 24 hauria de venir dissabte de 00 a dilluns 24:00
                next_end=timedelta(days=1)
            next_start = DateTime.combine((last_start + delta).date(), Time.min).replace(tzinfo=UTC)
        else:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup:
                # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            elif next_start.time() != Time.min:
                # If earliest does not fall on 5am skip to the next day. Per culpa d'això no es fa el primer?
                next_day = next_start.date() + timedelta(days=1)
                next_start = DateTime.combine(next_day, Time.min).replace(tzinfo=UTC)
                if next_start.weekday in (5):
                    next_end=timedelta(days=3)
                else:
                    next_end=timedelta(days=1)
            #a sota no hauria d'entrar mai?
            next_start_weekday = next_start.weekday()
            if next_start_weekday in (6, 1):  # If next start is in Sunday, monday, go to next tuesday. QUE AQUEST EN REALITAT ÉS DE DISSABTE A LES 00 AL MONDAY A LES 24:00
                #delta = timedelta(days=(7 - next_start_weekday)) # si estem al dissabte serà d'aqui dos dies i al diumenge d'aqui un
                #hauriem de començar el 5 a les 00 amb timedelta3
                delta = timedelta(days = (5-next_start_weekday))
                next_start = next_start + delta
                next_end=timedelta(days=3)
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=(next_start + next_end))


class WeekendsTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [WeekendsTimetable]