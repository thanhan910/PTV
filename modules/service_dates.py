import pandas as pd

def get_dates(monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date):
    # Get list of dates based on week pattern and date range
    week_pattern = [bool(int(monday)), bool(int(tuesday)), bool(int(wednesday)), bool(int(thursday)), bool(int(friday)), bool(int(saturday)), bool(int(sunday))]
    start_date = pd.to_datetime(start_date, format='%Y%m%d')
    end_date = pd.to_datetime(end_date, format='%Y%m%d')
    dates = pd.date_range(start_date, end_date)
    return dates[[week_pattern[i] for i in dates.dayofweek]]
    
def get_dates_df_calendar(calendar_df: pd.DataFrame, calendar_dates_df: pd.DataFrame):
    
    weekdate_columns = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    daterange_columns = ['start_date', 'end_date']
    pattern_columns = weekdate_columns + daterange_columns
    
    # Drop duplicates to reduce the number of rows to be processed
    df_dates = calendar_df[pattern_columns].drop_duplicates()
    
    # Get date list based on week pattern and date range
    df_dates['date'] = df_dates.apply(lambda x: get_dates(x['monday'], x['tuesday'], x['wednesday'], x['thursday'], x['friday'], x['saturday'], x['sunday'], x['start_date'], x['end_date']), axis=1)

    df_dates['date'] = df_dates['date'].apply(lambda x: [y.strftime('%Y%m%d') for y in x])
    
    # Join the date list with the original calendar table
    df_dates = pd.merge(calendar_df, df_dates, on=pattern_columns, how='left')
    
    # Explode the date list into separate rows
    df_dates = df_dates[['service_id', 'date']].explode('date')

    # Join the date df with the calendar_dates df
    df_dates = pd.merge(df_dates, calendar_dates_df.astype({'date': str, 'exception_type': str}), on=['service_id', 'date'], how='outer')
    
    # Drop 2 and keep 1 and NaN
    df_dates = df_dates[df_dates['exception_type'] != '2'].reset_index(drop=True)

    return df_dates
