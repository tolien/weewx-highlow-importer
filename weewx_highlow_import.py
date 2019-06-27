#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import sqlite3 as sqlite
import sys

"""
Script for exporting the wview solar radiation highlow table from SQLite
importing it into weewx's daily archive table
"""

WVIEW_HILOW_DB="/var/lib/wview/archive/wview-hilow.sdb"
WEEWX_DB="/var/lib/weewx/weewx.sdb"

def main():
    query = "SELECT dateTime, low, timeLow, high, timeHigh, cumulative, samples FROM solarRadiation ORDER BY datetime ASC"
    archive_day_timestamp = 0
    to_insert = []
    newrow = {}
    last_datetime = 0
    for row in _query_wview_sqlite(query):
        row_dict = _turn_row_into_dict(row)
        timestamp = math.floor(row_dict['datetime'] / 86400.0) * 86400
        print("%s clamped to %s" % (row_dict['datetime'], timestamp))
        if timestamp != archive_day_timestamp:
            if last_datetime == 0:
                last_datetime = row_dict['datetime']
                continue
            if archive_day_timestamp > 0:
                newrow['sumtime'] = timestamp - archive_day_timestamp 
                to_insert.append(newrow)
            newrow = {  'dateTime': timestamp,
                    'min': 0,
                    'mintime': 0,
                    'max': 0,
                    'maxtime': 0,
                    'count': 0,
                    'sum': 0,
                    'wsum': 0,
                    'sumtime': 0
                    }
            archive_day_timestamp = timestamp
        if row_dict['min'] <= newrow['min']:
            newrow['min'] = row_dict['min']
            newrow['mintime'] = row_dict['whenmin']
        if row_dict['max'] >= newrow['max']:
            newrow['max'] = row_dict['max']
            newrow['maxtime'] = row_dict['whenmax']
        newrow['sum'] = newrow['sum'] + row_dict['cumulative']
        interval = (row_dict['datetime'] - last_datetime) / row_dict['samples']
        #print("Archive interval: %s" % interval)
        newrow['wsum'] = newrow['wsum'] + (row_dict['cumulative'] * interval)
        newrow['count'] = newrow['count'] + row_dict['samples']
        last_datetime = row_dict['datetime']
    print (timestamp, archive_day_timestamp)
    newrow['sumtime'] = last_datetime - archive_day_timestamp
    to_insert.append(newrow)
    #_query_weewx_sqlite("DELETE FROM archive_day_radiation")
    records = []
    for new_row in to_insert:
        records.append((
            new_row['dateTime'], 
            new_row['min'], new_row['mintime'], 
            new_row['max'], new_row['maxtime'],
            new_row['sum'], new_row['count'],
            new_row['wsum'], new_row['sumtime']))
        print(new_row)
    print("%s records to insert" % len(records))
    query = """INSERT INTO archive_day_radiation 
        (dateTime, 
        min, mintime, 
        max, maxtime, 
        sum, count, 
        wsum, sumtime) VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    #result = _querymany_weewx_sqlite(query, records)

def _turn_row_into_dict(row):
    row_dict = {}
    row_dict['datetime'] = row[0]
    row_dict['min'] = row[1]
    row_dict['whenmin'] = row[2]
    row_dict['max'] = row[3]
    row_dict['whenmax'] = row[4]
    row_dict['cumulative'] = row[5]
    row_dict['samples'] = row[6]

    return row_dict

def _query_wview_sqlite(query):
    con = None
    try:
        con = sqlite.connect(WVIEW_HILOW_DB)
        cur = con.cursor()
        cur.execute(query)
        result = cur.fetchall()
        return result

    except sqlite.Error as e:
        print("Error fetching SQLite data %s" % e.args[0])
        sys.exit(1)

    finally:
        if con:
            con.close()

def _query_weewx_sqlite(query):
    con = None
    try:
        con = sqlite.connect(WEEWX_DB)
        cur = con.cursor()
        #print("Executing %s" % query)
        cur.execute(query)
        result = cur.fetchall()
        return result

    except sqlite.Error as e:
        print("Error fetching SQLite data %s" % e.args[0])
        sys.exit(1)

    finally:
        if con:
            con.commit()
            con.close()
def _querymany_weewx_sqlite(query, data):
    con = None
    try:
        con = sqlite.connect(WEEWX_DB)
        cur = con.cursor()
        #print("Executing %s" % query)
        cur.executemany(query, data)

    except sqlite.Error as e:
        print("Error fetching SQLite data %s" % e.args[0])
        sys.exit(1)

    finally:
        if con:
            con.commit()
            con.close()
if __name__ == "__main__":
    main()

