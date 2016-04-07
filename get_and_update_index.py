#! /usr/bin/env python
# coding:utf-8

import tushare as ts
import os
import datetime

# 上证50, 创业板指
index_map = {'000016':'2004-01-02', '399006':'2010-05-31'}

# Data file dir
FILE_NAME = 'data/{:0>6}.csv'

## get [s_time, e_time)'s index data, sort by date and append to file
def get_index_data(idx, s_time, e_time):

	file_name = FILE_NAME.format(idx)
	start_time = datetime.datetime.strftime(s_time, "%Y-%m-%d")
	end_time = datetime.datetime.strftime(e_time, "%Y-%m-%d")

	print 'start:'+start_time, 'end:'+end_time, 'file:'+file_name, "\n"

	df = ts.get_h_data(idx, index=True, start=start_time, end=end_time)
	if df is None :
		print "No latest index data until to now!!!"
		return

	if os.path.exists(file_name):
		df.sort().to_csv(file_name, mode='a', header=None)
	else:
		df.sort().to_csv(file_name)


## get all data of index by step, 1 year as a gap due to tushare
def get_long_time_data(idx):
	idx_start = datetime.datetime.strptime(index_map[idx], "%Y-%m-%d")
	idx_end = datetime.datetime.now()

	idx_st = idx_start
	idx_gap = idx_st + datetime.timedelta(365)
	while idx_end >= idx_gap:
		get_index_data(idx, idx_st, idx_gap)
		idx_st = idx_gap + datetime.timedelta(1)
		idx_gap = idx_st + datetime.timedelta(365)

	if idx_end > idx_st :
		get_index_data(idx, idx_st, idx_end)


## get the last record time of index data file
def get_last_record_time(idx):
	
	file_name = FILE_NAME.format(idx)

	# simple to get last line, only for data file
	with open(file_name, 'rb') as fh:
		fh.seek(-200, 2)
		lines = fh.readlines()
		last = lines[-1]

	return last.split(',')[0]
	

## update the index data file to now
def update_idx_data(idx):
	idx_current = datetime.datetime.strptime(get_last_record_time(idx), "%Y-%m-%d") + datetime.timedelta(1)
	idx_now = datetime.datetime.now()

	if idx_now > idx_current :
		get_index_data(idx, idx_current, idx_now)


# get all data of index in map for init
#for idx in index_map.keys():
#	get_long_time_data(idx)


# update index in map
#for idx in index_map.keys():
#	update_idx_data(idx)


for idx in index_map.keys():
	file_name = FILE_NAME.format(idx)
	if os.path.exists(file_name):
		update_idx_data(idx)
	else:
		get_long_time_data(idx)


