# Historic Weather Data Study
This is a scalable computing project that I have done as part of the course curriculum in pyspark

DATA
In this assignment we will study some of the weather data contained in the Global Historical Climate
Network (GHCN), an integrated database of climate summaries from land surface stations around the
world. The data covers the last 175 years and is collected from more than 20 independent sources,
each of which have been subjected to quality assurance reviews.
• Global Historical Climatology Network (GHCN)
• GHCN Daily
The daily climate summaries contain records from over 100,000 stations in 180 countries and territories
around the world. There are several daily variables, including maximum and minimum temperature, total
daily precipitation, snowfall, and snow depth; however, about half of the stations report precipitation only.
The records vary by station and cover intervals ranging from less than a year to 175 years in total.
The daily climate summaries are supplemented by metadata further identifying the stations, countries,
states, and elements inventory specific to each station and time period. These provide human readable
names, geographical coordinates, elevations, and date ranges for each station variable in the inventory.
Daily
The daily climate summaries are comma separated, where each field is separated by a comma ( , ) and
where null fields are empty. A single row of data contains an observation for a specific station and day,
and each variable collected by the station is on a separate row.
The following information defines each field in a single row of data covering one station day. Each field
described below is separated by a comma ( , ) and follows the order below from left to right in each row.
Name Type Summary
ID Character Station code
DATE Date Observation date formatted as YYYYMMDD
ELEMENT Character Element type indicator
VALUE Real Data value for ELEMENT
MEASUREMENT FLAG Character Measurement Flag
QUALITY FLAG Character Quality Flag
SOURCE FLAG Character Source Flag
OBSERVATION TIME Time Observation time formatted as HHMM
The specific ELEMENT codes and their units are explained in Section III of the GHCN Daily README,
along with the MEASUREMENT FLAG, QUALITY FLAG, and SOURCE FLAG. The OBSERVATION
TIME field is populated using the NOAA / NCDC Multinetwork Metadata System (MMS).
Metadata
The station, country, state, and variable inventory metadata files are fixed width text formatted, where
each column has a fixed width specified by a character range and where null fields are represented by
whitespace instead.
Page 1 of 8Assignment 1 DATA420-20S1 (C)
Stations
The stations table contains geographical coordinates, elevation, country code, state code, station name,
and columns indicating if the station is part of the GCOS Surface Network (GSN), the US Historical
Climatology Network (HCN), or the US Climate Reference Network (CRN). The first two characters of
the station code denote the country code (FIPS).
Name Range Type
ID 1 - 11 Character
LATITUDE 13 - 20 Real
LONGITUDE 22 - 30 Real
ELEVATION 32 - 37 Real
STATE 39 - 40 Character
NAME 42 - 71 Character
GSN FLAG 73 - 75 Character
HCN/CRN FLAG 77 - 79 Character
WMO ID 81 - 85 Character
Countries
The countries table contains country name only.
Name Range Type
CODE 1 - 2 Character
NAME 4 - 50 Character
States
The states table contains state name only.
Name Range Type
CODE 1 - 2 Character
NAME 4 - 50 Character
Inventory
The inventory table contains the set of elements recorded by each station, along with the time period
each element was recorded. The specific ELEMENT codes and their units are explained in Section III
of the GHCN Daily README.
Name Range Type
ID 1 - 11 Character
LATITUDE 13 - 20 Real
LONGITUDE 22 - 30 Real
ELEMENT 32 - 35 Character
FIRSTYEAR 37 - 40 Integer
LASTYEAR 42 - 45 Integer
Note that the latitude and longitude correspond to the latitude and longitude in the stations table exactly.
Page 2 of 8Assignment 1 DATA420-20S1 (C)
TASKS
The assignment tasks are separated into sections, each of which explore the data in increasing
detail. In the first section you will explore the metadata tables and part of the daily climate summaries to
help develop your code without waiting all day. In the second section you will answer specific questions
about the data using the code you have developed. 
