#!/usr/bin/env python

import datetime
import calendar

print("""
// Auto-generated mktime mapping file

#include "mktime.h"

int mkTime[] = {""")

for year in range(1970,2070):
  for month in range(1,13):
    d = datetime.datetime(year, month, 1)
    sec = calendar.timegm(d.timetuple())
    print('  %li, // %04i-%02i' % (sec, year, month))

print('  0};')

