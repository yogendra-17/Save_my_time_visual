import pandas as pd
import traceback
import json

# TODO BUG : if an activity stars at 11 pm on day1 and ends at 1 am next day (day2)
# the activity is counted as if it ended on 1 am of day1.
# eg check 260-06-2018 , Sleep activity.

# TODO : add feature to change the start time of the day manually
# eg i would want to set my day started at 6 am and ending next day 5:59 am
# if i want to see my sleep data.
class smtData:
    def __init__(self, init_dataframe):
        """

        :param init_dataframe: raw dataframe given at time of initiation (it's sorted by start time):
            activityName              object
            activityCategoryName      object
            activityStartDate [ms]     int64
            activityStartDate         object
            activityEndDate [ms]       int64
            activityEndDate           object
            activityDuration [ms]      int64
            activityDuration          object
            dtype: object
        """
        self._df = init_dataframe

        self._drop_redundant()
        self._rename_columns()
        self._ms_to_datetime()

        self.activities = list(self._df[['category','activity']]
                               .drop_duplicates()
                               .itertuples(index=False, name=None))

    def __repr__(self):
        return f"{self._df}"

    def _drop_redundant(self):
        """
        drops redundant columns (including both the duration columns)
        :return: self
        """
        redundant_columns = ['activityStartDate', 'activityEndDate', 'activityDuration', 'activityDuration [ms]']
        try:
            self._df = self._df.drop(redundant_columns, axis='columns')
            return self
        except:
            print(traceback.format_exc())

    def _rename_columns(self):
        columns_dict = {'activityStartDate [ms]': 'start', 'activityName': 'activity',
                        'activityCategoryName': 'category', 'activityEndDate [ms]': 'end'}
        try:
            self._df = self._df.rename(columns_dict, axis='columns')
            return self
        except:
            print(traceback.format_exc())

    def _ms_to_datetime(self):
        self._df['start'] = (pd.to_datetime(self._df['start'], unit='ms')
                             .dt.tz_localize('UTC')
                             .dt.tz_convert('Asia/Kolkata')
                             )
        self._df['end'] = (pd.to_datetime(self._df['end'], unit='ms')
                           .dt.tz_localize('UTC')
                           .dt.tz_convert('Asia/Kolkata')
                           )

    def reindex(self, by):
        """
        reindex the dataframe.
        :keyword by: string: 'start_time', 'category_activity'
        :return: smt_Data object
        """

        self._df = (
            # to get the initial four-column dataframe
            self._df.reset_index(drop=True)
        )

        if by == 'start_time':
            self._df.index = pd.DatetimeIndex(self._df['start'])

        elif by == 'category_activity':
            self._df = (
                self._df.set_index(self._df['category'], append=True)
                    .set_index(self._df['activity'], append=True)
                    .reset_index(level=0, drop=True)  # removes the integer index
            )

        return self

    def get_time_sheet_data(self, activity):
        """

        :param activity: tuple containing two strings. ('category1', 'activity1')
        :return: dataframe with index ['date'] and column ['start_end_min']
        """
        timesheet = self.reindex(by='category_activity')._df.loc[
            activity]  # reindex the dataframe by Category, Activity

        timesheet.loc[:, 'date'] = timesheet['start'].dt.date
        timesheet.loc[:, 'start_min_of_day'] = timesheet['start'].dt.hour * 60 + timesheet['start'].dt.minute
        timesheet.loc[:, 'end_min_of_day'] = timesheet['end'].dt.hour * 60 + timesheet['end'].dt.minute
        timesheet.loc[:, 'day_from_start'] = (timesheet['date'] - timesheet['date'][0]).dt.days

        # Creating column of list of lists: [[start_min1, end_min1], [start_min2, end_min2]]
        # one such list of lists for each day when the activity occurs
        # using 'start_min_of_day' and 'end_min_of_day' columns
        timesheet.loc[:, 'start_end_min'] = (
            timesheet.loc[:, ['start_min_of_day', 'end_min_of_day']]  # joins two columns
                .apply(list, axis='columns')
        )
        timesheet = (timesheet[['date', 'day_from_start', 'start_end_min']].groupby(by=['date'])
                     .agg({'start_end_min': list,  # merges all rows having same date
                           'day_from_start': lambda x: x[0]})
                     )

        return timesheet


if __name__ == "__main__":
    import pprint
    pd.set_option('display.expand_frame_repr', False)

    smt = smtData(pd.read_csv("Data\\09 5 June 2019.csv"))
    smt = smt.get_time_sheet_data(('Rest', 'Sleep')).to_dict(orient='index')

    pprint.pprint(smt)
