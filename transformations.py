from datetime import datetime, timedelta
import pandas as pd

def calculate_speed(previous_breadcrumb, current_breadcrumb):
    distance = current_breadcrumb['METERS'] - previous_breadcrumb['METERS']
    time_difference = current_breadcrumb['ACT_TIME'] - previous_breadcrumb['ACT_TIME']
    speed = distance / time_difference if time_difference != 0 else 0
    return speed

def decode_timestamp(breadcrumb):
    opd_date = pd.to_datetime(breadcrumb['OPD_DATE'],format='%d%b%Y:%H:%M:%S')
    timestamp = opd_date + pd.to_timedelta(breadcrumb['ACT_TIME'], unit='s')
    return str(timestamp)

