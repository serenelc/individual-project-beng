"""
I need the TfL predicted journey times between 2 stops at the same request times as I have 
in the historical model i.e. every 30 minutes (but this could be dropped to every 15 minutes)
so to be safe, I need the TfL predicted journey times between 2 stops every 15 minutes from the hour.

TfL gives predicted arrival times, so in order to get predicted journey times I need to do the following:
- I am at stop A and I want to get the predicted journey time from stop A to stop B
- request Countdown for stop A and stop B.
- get the time of the earliest bus that will leave A.
- find the corresponding vehicle in the Countdown response for stop B
- The difference in these times is TfL's predicted journey time for a bus from stop A to stop B 
at this request time.

For these specific stops and routes
1) 52: Chesterton Road -> Nottinghill Gate
2) 52: Willesden Bus Garage -> Okehampton Road
3) 52: Willesden Bus Garage -> Harrow Road / Kilburn Lane
4) 52: Willesden Bus Garage -> Chesterton Road
5) 52: Willesden Bus Garage -> Notting Hill Gate Station
6) 52: Willesden Bus Garage -> Palace Gate
7) 52: Willesden Bus Garage -> Knightsbridge Station / Harrods
8) 52: Willesden Bus Garage -> Victoria Bus Station
9) 9: North End Road -> Phillimore Gardens
10) 52: All Souls Avenue -> Notting Hill Gate Station
11) 69: Florence Road -> Star Lane
"""

import datetime as dt
import time
import os
import json
# import psycopg2
from local_helper import Utilities
# from local_data_collection import Data_Collection
from urllib.error import HTTPError, URLError

def main():
    helper = Utilities()
    

