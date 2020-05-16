## TO DO

- Need to update the README with instructions that are valid
- route 9 bus 14511 has its directions opposite to what it should be idk why, but all the othe rbuses seem to have the right direction of travel. 
- Update diagram in final report
- do some sort of api call that gets all the stops on a route in order both inbound and outbound


## Overview

Functions to do with data processing are in the data_collection.py file. Utility functions (e.g. writing to CSVs) are in the helper.py file.


### Timetable
Mon - Friday/Monday - Thursday
- 7: First bus 5.25, Last bus 23.40, Oxford Circus Stn/Harewood Place -> Long Drive ~46 min,  Brunel Road -> Oxford Circus Station/John Lewis ~ 46 min
- 277: First bus 5.00, Last bus 00.06, Crossharbour Asda -> Dalston Junction Station ~52 min, Dalston Junction Station -> Isle of Dogs Asda ~51 min
- 9: First bus 05.35, Last bus 23.50, Aldwych/Somerset House -> Hammersmith Bus Station ~47 min, Hammersmith Bus Station -> Aldwych/Drury Lane ~48 min
- 52: 24 hours, Victoria Station -> Pound Lane ~52 min, Pound Lane/Willesden Bus Garage -> Victoria Bus Station ~57 min
- 452: First bus 05.05, Last bus 23.55, 
- 328: First bus 04.50, Last bus 00.35
- 267: First bus 05.50, Last bus 00.30
- 35: First bus 03.55, Last bus 00.15
- 37: 24 hours
- 69: 24 hours
- 6: 24 hours
- 14: 24 hours

### Objective

The code should concurrently run the data collection functions for 5-10 bus routes for a minimum of a month, 24 hours a day. The code calls the APIs every 30 seconds because that is how often the information gets updated at the source, so there is no point calling the API anymore often. Each bus route should store the arrival information in its own CSV file for each 24 hour day. A new CSV file should be made for each new day, starting at 00:00. 

### What the code does

Read the Latex Final Report
