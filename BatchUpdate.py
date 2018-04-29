import os		#operating system stuff
import re		#regular expressions
import requests #simplifies network calls.  #http://docs.python-requests.org
import sys 		#interpreter functions and variables
import time		#for timing processes
import xml.etree.ElementTree as ET	#handling xml data from Alma

""" main()
	The function main() pulls out the arguments passed through the command
	prompt and determines which functionality to execute based on flags.  This
	allows for progressive checks to verify data isn't being mishandled. So far
	there will be programmed:
		1) -f(ormat): takes a csv file of items pulled out of Alma, and removes
		all extraneous information.  The input csv should be preformatted to not
		include any commas besides the delimiters. This only outputs a single
		file using the format 'formatted-inputfile'
		2) -s(plit): takes a csv file, updates columns according to defaults
		given, and runs a set of regular expressions on the description field to
		extract its information.
		3) -u(pdate): takes a csv file and will push the each item's
		information back into Alma according to the the 
		   
		 ******************************* WARNING *******************************
		 *   This program heavily relies on the CSV format, which depends on   *
		 *    both commas and newline characters when interpreting the file.   *
		 *  Stray commas or newlines can break the program. It is recommended  *
		 * to do the following steps:                                          *
		 *                                                                     *
		 *   1) Open the original file as an Excel spreadsheet (not csv).      *
		 *	Search and replace all commas with '' (empty string). Ensure   *
		 *      none of the data is kept in a named table. That messes stuff   *
		 *      up for some reason.                     		       *
		 *   2) Save the file in the csv format, and open it this time in a    *
		 *	text editor that can handle regular expressions (e.g. 	       *
		 *	Notepad++). Do a regex replace of '\n\s\s+' with '\s'. This    *
		 *	will replace the typical indented newline that is not 	       *
		 *	associated with CSV formatting. Save over the csv file.        *
		 *   3) You should now be able to use the program without breaking it. *
		 *	If you still somehow managed to break it, congratulations!     *
		 *	Contact Brendan Deroo (bderoo@bu.edu) for help fixing it.      *
		 ***********************************************************************
"""

# Tuples designate the column name, flags indicating how to process the
# item, and optionally what the default value of items should be when
# being updated. If the flags contain an 'n', it means the field contains
# numbers and ensures a "'" is prepended to it to prevent buggy CSV
# silliness. 't' is for plain text fields. 'x' is for fields that should
# not be prompted for because they are calculated from other fields.
# Multiple flags can be used in a string.
#
# Including 'Description' in the add or opt fields signals the enumeration
# and chronology analyzer should run. 'Pattern' is always appended in
# split if not present. 'Notes' is always appended in split and update if
# not present.
mand = [('MMS ID','n'),('Barcode','n'),('title','t')]
opt = []
add = [('Material Type','t','Bound Issue'),('Item Policy','t','non-circulating'),('Description','tx')]
		
# The code tables are used when running checks on column values and
# uploading data into Alma. Keys in code_tables represent columns that have
# limited possibilities, stored as dicts.  Under the assumption that
# different codes will always have different descriptions, each column's
# code table is stored reversed as value:code pairs for better
# understandability. (The CSV data outputted by Alma displays the value, but
# uploading into Alma requires you to use the code).
# The code tables are by no means complete.  I've filled out the most
# frequently used options, but checks should inform user to update the table
# if their value isn't accepted.
""" TODO: Consider pulling data tables from Alma whenever the check is
	initialized, just in case values ever change."""
code_tables = {'Status':{
					'Item not in place': '0',
					 'Item in place': '1'},
			   'Material Type':{
					'Book': 'BOOK',
					'Compact Disc': 'CD',
					'CD-ROM': 'CDROM',
					'Computer Disk': 'DISK',
					'DVD': 'DVD',
					'DVD-ROM': 'DVDRM',
					'Bound Issue': 'ISSBD',
					'Issue': 'ISSUE',
					'Thesis': 'THESIS'},
				'Item Policy':{
					'general circulation': '0',
					'non-circulating': '1',
					'24-hour circulation': '2',
					'3-day circulation': '3',
					'7-day circulation': '4',
					'2-hour library use only': '5',
					'3-hour video': '6',
					'6-hour media loan': '7',
					'WCat-ShortLoan': '30',
					'WCat-LongLoan': '31',
					'3-hour loan': '33'},
				'Process type':{
					'Acquisition': 'ACQ',
					'Loan': 'LOAN',
					'Claimed Returned': 'CLAIM_RETURNED_LOAN',
					'Lost': 'LOST_LOAN',
					'Hold Shelf': 'HOLDSHELF',
					'Transit': 'TRANSIT',
					'In Process': 'WORK_ORDER_DEPARTMENT',
					'Missing': 'MISSING',
					'Technical - Migration': 'TECHNICAL',
					'Resource Sharing Request': 'ILL',
					'Requested': 'REQUESTED',
					'In Transit to Remote Storage': 'TRANSIT_TO_REMOTE_STORAGE'}}

def main():
	"""TODO: Add check for when script is working off a prefixless filename but
		a prefixed one is present in the location."""
	
	if len(sys.argv) < 3:
		print("usage: BatchUpdate.py inputCSVorTXT {-f|-s|-u}")
		sys.exit(1)
	
	filename = sys.argv[1]
	flags = sys.argv[2:]
	
	if '-f' in flags:
		filename = format(filename)
	if '-s' in flags:
		filename = split(filename)
	if '-u' in flags:
		if ('-f' in flags) or ('-s' in flags):
			text = str(input("Are you sure you want to update without reviewing the data? (Y/N) "))
			print(text.upper())
			if text.upper() != 'Y':
				print("Halting processes")
				sys.exit(1)
		filename = update(filename)
	if ('-f' not in flags) and ('-s' not in flags) and ('-u' not in flags):
		print("usage: BatchUpdate.py inputCSV {-f|-s|-u}")
		sys.exit(1)
		
""" format()
		-takes a csv file of items pulled out of Alma, and removes all
		extraneous information.  The input csv should be preformatted to not
		include any commas besides the delimiters. This only outputs a single
		file using the format 'f_inputfile'
"""	
def format(filename):
	print("Formatting "+filename+"...")
	output = []
	data = _readFile(filename)
	
	
	# Add columns to the original data that need to be present, retrieve indexes
	# for referencing, and record special columns needing formatting.
	(data,ind,nums,derived) = _checkColumns(data,mand,opt,add)

	for row in data:
		item = []
		# Check every column recorded within the _checkColumns function.
		for col in ind:
		
			# Prepend a "'" if the column is numerical.  This prevents a bug in
			# CSV data which can lossily save over large numbers that it
			# reinterprets in scientific notation
			#     ( 11719123456789 -> 1.171E13 -> 11710000000000 )
			# Check whether the column is a number that needs special handling.
			# Ignore the first row. It shouldn't need special handling.
			if (col in nums) and (data.index(row)>0):
				item.append("'" + row[ind[col]])
			else:
				item.append(row[ind[col]])
				
		output.append(item)

	# Write to a new file using the provided filename with a prefix.
	# Checks for and eliminates already attached prefixes 
	new_filename = _writeTo('f_', output)
	return new_filename
	
"""split()
		- Takes a list of items and pass each through a series of regular
		expressions to pull out necessary information.  It will write the
		results out to multiple csv files, one that includes all the information
		used in updating, and the regular expression that captured it, and one
		that contains items not matching any of the defined regular expressions.
		- Does not assume the file has been previously formatted, only that
		certain necessary columns are present.
		- Does not attempt to filter out items with anomalous information (i.e.
		"i" barcodes or missing data; this is handled by update()
		
		TODO: Ensure duplicate columns are handled
		
"""	
def split(filename):

	print("Splitting "+filename+"...")
	
	
	# Read the file into useable data
	data = _readFile(filename)
	
	# Add columns to the data that need to be present, retrieve indexes
	# for referencing, and record columns with formatting.
	"""TOOD: Check whether num or derived is needed"""
	(data, ind, num, derived) = _checkColumns(data, mand, opt, add)
	
	# Always add the "Pattern" and "Notes" columns if they aren't present.
	if "Pattern" not in data[0]:
		for row in data:
			row.append("")
		data[0][-1] = "Pattern"
	ind["Pattern"] = data[0].index("Pattern")
	
	if "Notes" not in data[0]:
		for row in data:
			row.append("")
		data[0][-1] = "Notes"
	ind["Notes"] = data[0].index("Notes")
	
	# For optional columns present in the data, fetch a value to overwrite all
	# **blank** entries with. When a default was given, use that, otherwise
	# prompt the user for a value. If the column is not present, do nothing.
	# Also will ignore any column with the 'x' ignore flag.
	for opt_col in opt:
		opt_colname = opt_col[0]
		if (opt_colname in data[0]) and ('x' not in opt_col[1]):
			
			message = "How should '" + opt_colname + "' be filled in?  *blank* --> "
			
			if (len(opt_col) == 3): # -> A default was given
				replacement = str(opt_col[2])
			else:
				replacement = str(input(message))
			
			# Check whether the column is one that can only support the limited
			# number of options given in code_tables
			"""TODO: Implement network checks for whether there is a code table 
				for every column."""
			if opt_colname in code_tables:
				#-> Check replacement against possible values until a valid
				#   option is found. Update all blank values with the
				#   replacement.
				replacement = _checkValue(opt_colname, replacement, message)
					
				for row in data[1:]:
					if row[ind[opt_colname]] == "":
						row[ind[opt_colname]] = replacement
	
	# For the add-in columns, fetch a value to overwrite **all entries** with.
	# When a default was given, use that, otherwise prompt the user for a value.
	# The previous call to _checkColumns guarantees these columns' existence.
	# Also will ignore any columns with the 'x' ignore flag
	for add_col in add:
		add_colname = add_col[0]
		if ('x' not in add_col[1]):
			
			message = "How should all items in '" + add_colname + "' be filled in? "
			
			if (len(add_col) == 3): # -> A default was given
				replacement = str(add_col[2])
			else:
				replacement = str(input(message))
					
			# Check whether the column is one that can only support the limited
			# number of options given in code_tables
			"""TODO: Implement network checks for whether there is a code table 
				for every column."""
			if add_colname in code_tables:
				#-> Check replacement against possible values until a valid
				#   option is found. Update all values with the replacement.
				replacement = _checkValue(add_colname, replacement, message)
				
				for row in data[1:]:
					row[ind[add_colname]] = replacement
	
	# If the 'Description' field is present in the index list, run the
	# enumeration and chronology parser.
	if 'Description' in ind:
		data = _matchDescriptions(data,ind)
		
	# Sort the items by their bib-level ids ('MMS ID').
	vol_pattern = re.compile('^(?:[sS][eE][rR]\.?\s*)?(\d+)?\s*(?:[vV][oO]?[lL]?\.?\s*)(\d+)')
	def sort_key(row):
		if 'Description' in ind:
		
			#   If there is a 'Description', further sort the items by their
			# pattern-matched volume information, then the whole description.
			# The volume information is extracted and processed using a regex
			# pattern so that items are sorted numerically rather than by 
			# character (such that v10 comes after v2).
			match = vol_pattern.search(row[ind["Description"]])
			if match == None:
				result = (row[ind["MMS ID"]], 0, 0, row[ind["Description"]])
			else:
				if match.group(1) == None:
					preVol = 0
				else:
					preVol = int(match.group(1))
				if match.group(2) == None:
					volInfo = 0
				else:
					volInfo = int(match.group(2))
					
				result = (row[ind["MMS ID"]], preVol, volInfo, row[ind["Description"]])
		else:
			result = row[ind["MMS ID"]]
		return result
	data = data[:1] + sorted(data[1:], key=sort_key)
	
	#Run various other tests and processes on each item to clean them up.
	#Currently programmed are Barcode checks, material type check, Chron I
	#smartguessing, and Chron J reformatting (so far).
	"""	
		TODO: Make notes on words found in description, like 'inc.' or 'index'
	"""
	
	# Run tests on the barcodes:
	for row in data[1:]:
		barcode = row[ind["Barcode"]]
		# TEST: Lacking barcode
		if (barcode == "'") or (barcode == None):
			row[ind["Notes"]] += ("; ","")[row[ind["Notes"]] == ''] + "Err: Missing barcode"
		# TEST: i-barcodes
		if (len(barcode)>2) and (barcode[1] == 'i'):
			row[ind["Notes"]] += ("; ","")[row[ind["Notes"]] == ''] + "Err: i-barcode"
			
	# Description specific tests:
	"""TODO: Look into ways to merge this description test block with the
		previous one"""
	if "Description" in ind:
	
		# Set up some structures and patterns to be used for the upcoming tests.
		start_year_pat = re.compile('^(\d+)(.*)')
		months = [('Jan','(ja\w*)',), ('Feb','(fe\w*)'),('Mar','(ma*r\w*)'),
		  ('Apr','(ap\w*)'),('May','(ma*y)'),('Jun','(j(?:une|un|n|e))'),
		  ('Jul','(j(?:uly|ul|l|y))'),('Aug','(au?g\w*)'),('Sep','(se\w*)'),
		  ('Oct','(oc\w*)'),('"','(no?v\w*)'),('Dec','(de\w*)'),
		  ('Spr','(spr\w*)'),('Sum','(su\w*)'),('Fal','(fa\w*|au(?!thor|g)\w*)'),
		  ('Win','(wi\w*)')]
		month_patterns = [(month[0], re.compile(month[1], re.I)) for month in months]
	
		for i in (range(1,len(data))):
		
			# TEST: Chron_I "smart guess". Looks for year data encapsulated in
			# two digit numbers (e.g. Ap-Je98) and reinterprets it as a 4 digit
			# year depending on its neighbors. Requires the data to be sorted.
			"""
				TODO: Redo test interpreting the years entirely as years, or 
				  as numbers
				TODO: Handle residual question marks somehow
				TODO: Redo this entire mess.  I can't even understand what
				  is going on anymore
			"""
			year_match = start_year_pat.search(data[i][ind["Chron I"]])
			if year_match != None: # -> Year was provided
				year = year_match.group(1)
				remainder = year_match.group(2)
				if len(year) < 4: # -> Year needs to be reinterpreted
				
					# First, record the previous year in the range that has a valid
					# year format:
					prev_year = "?"
					j = 1
					while (prev_year == "?"):
						# Only check previous rows as long as they belong to the
						# same title (i.e. same MMS_ID) and aren't in the header or
						# an invalid row.
						if (i-j <= 0) or (data[i-j][ind["MMS ID"]] != data[i][ind["MMS ID"]]):
							break
						else:
							year_match = start_year_pat.search(data[i-j][ind["Chron I"]])
							# Record the year if it finds a 4 digit year.
							if (year_match != None) and (len(year_match.group(1)) == 4):
								prev_year = year_match.group(1)
							j += 1
				
					# Then record the next year in the range that has a valid year format:
					next_year = "?"
					j = 1
					while (next_year == "?"):
						# Only check next rows as long as they belong to the same
						# title (i.e. same MMS_ID) and aren't in an an out of range
						# row.
						if (i+j >= len(data)) or (data[i+j][ind["MMS ID"]] != data[i][ind["MMS ID"]]):
							break
						else:
							year_match = start_year_pat.search(data[i+j][ind["Chron I"]])
							# Record the year if it finds a 4 digit year.
							if (year_match != None) and (len(year_match.group(1)) == 4):
								next_year = year_match.group(1)
							j += 1

					if (prev_year != "?") and (next_year != "?"):
						"""TODO: This whole section again. blech"""
						# Try appending a number of digits from the previous and
						# next years until(prev year<=current year<=next year) makes
						# sense.  If it continues to not make sense, check that both
						# centuries are the same, and use thoes digits. Otherwise
						# raise an error.
						digits = 4-len(year) # In case e.g.  '05 -> "5", you'd use the first 3 digits.

						if int(prev_year) <= int(prev_year[:digits] + year) <= int(next_year):
							data[i][ind["Chron I"]] = prev_year[:digits] + data[i][ind["Chron I"]]
						elif int(prev_year) <= int(next_year[:digits] + year) <= int(next_year):
							data[i][ind["Chron I"]] = next_year[:digits] + data[i][ind["Chron I"]]
						else:
							# For adjacent centuries, test which interpretation is
							# closest to the average of the boundary years.
							test_centuries = [int(prev_year[:2])-1, int(prev_year[:2]), int(prev_year[:2])+1]
							avg_year = (int(prev_year) + int(next_year))/2
							avg_diff = [abs(avg_year - (int(cent)*100+int(year))) for cent in test_centuries]
							data[i][ind["Chron I"]] = str(test_centuries[avg_diff.index(min(avg_diff))]) + data[i][ind["Chron I"]]
					
					# If prev_year only remains unknown, guess based on the next
					# year
					elif (prev_year == "?") and (next_year != "?"):
						next_digits = int(next_year[-2:])
						current_digits = int(year)
						if current_digits > next_digits: #E.g. ?<'98<2003
							current_year = int(next_year) - next_digits - 100 + current_digits
						else: #E.g. ?<'95<1998 or ?<'43<1943
							current_year = int(next_year) - next_digits + current_digits
						data[i][ind["Chron I"]] = str(current_year)
						
					# If next_year only remains unknown, guess based on the
					# previous year
					elif (prev_year != "?") and (next_year == "?"):
						prev_digits = int(prev_year[-2:])
						current_digits = int(year)
						if prev_digits > current_digits: #	E.g. 1998<'03<?
							current_year = int(prev_year) - prev_digits + 100 + current_digits
						else: #	E.g. 1992<'95<? or 1943<'43<?
							current_year = int(prev_year) - prev_digits + current_digits
						data[i][ind["Chron I"]] = str(current_year)
					else:
						data[i][ind["Notes"]] += ("; ","")[data[i][ind["Notes"]] == ''] + "Err: Problem interpreting Chron I"
							
			# TEST: Assure proper ChronJ formats (three letter months/seasons,
			#	capitalized).
			for pat in month_patterns:
				data[i][ind["Chron J"]] = pat[1].sub(pat[0], data[i][ind["Chron J"]])
			
	# Write to a new csv file using the provided filename with a prefix
	# Check for and eliminate already attached prefixes	
	new_filename = _writeTo('s_', data)
	return new_filename
	
def update(filename):
	
	# The api keys have been removed to prevent accidental editing of working
	# records. Api keys can be requested from the Ex Libris developer site:
	# https://developers.exlibrisgroup.com/alma/apis
	
	sandbox_apikey = '####################################'
	active_apikey = '####################################'
	
	apikey = active_apikey


	# This is the shortcut API call that fetches item information using only a
	# barcode. Super helpful.
	fetchItemsUrl = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/items'
	
	# Read the file into useable data
	data = _readFile(filename)
	
	# Set up data containers (and headers) that will contain the items that
	# update successfully, and those that have errors or notes warning against
	# updating.
	success_data = []
	success_data.append(data[0])
	error_data = []
	error_data.append(data[0])

	#print("Updating " + filename)
	
	# Verify columns and fetch their locations. These will force close the program if they
	# are not present.

	(data, ind, nums, der) = _checkColumns(data, mand, opt, add)
	# Always add the "Notes" column if it isn't present.
	if "Notes" not in data[0]:
		for row in data:
			row.append("")
		data[0][-1] = "Notes"
	ind["Notes"] = data[0].index("Notes")
	
	# Note the location of a "Pattern" column if it is present.
	if "Pattern" in data[0]:
		ind["Pattern"] = data[0].index("Pattern")
	
	numItems = len(data[1:])
	ts0 = time.time()
	for i in range(numItems):
		row = data[i+1]
		print("Processing item " + str(i+1) + " of " + str(numItems))
		# First, weed out the items previously identified to have problems.
		if row[ind["Notes"]].find("Err") > -1:
			error_data.append(row)
			print("  -Skipped, item has error")
		elif ("Pattern" in ind) and (row[ind["Pattern"]]=="N/A"):
			error_data.append(row)
			print("  -Skipped, item's description could not be matched")
		else:
			barcode = row[ind["Barcode"]][1:] # To account for apostrophes
			
			# Fetch the item data from Alma
			alma_request = requests.get(fetchItemsUrl, params = {'apikey':apikey, 'item_barcode':barcode})
			
			# Catch cases where data was not successfully fetched
			if (alma_request.status_code != 200):
				error_data.append(row)
				error_data[-1][ind["Notes"]] = "Err: Problem fetching item information. Code " + str(alma_request.status_code)
			else:
				root = ET.fromstring(alma_request.text)
				
				#Fetch the URL that will push data back into Alma
				updateUrl = root.get('link')
				item_data = root.find('item_data')
				
				"""
					#TODO: Update this to reflect the additional and optional columns
					#TODO: Add a column name to column_key conversion table
				"""
				# Edit the Material Type field if it exists, create it
				# otherwise. Note Material Type has a code table included at the
				# beginning of this document.  The keys within are the publicly
				# visible descriptions, and the keys' values are what is needed
				# to update the code properly.
				if (ind["Material Type"] > -1):
					mattype_element = item_data.find('physical_material_type')
					if mattype_element == None:
						# -> Field doesn't exists in data: add child to XML
						mattype_element = ET.SubElement(item_data,'physical_material_type')
					mattype_element.text = code_tables["Material Type"][row[ind["Material Type"]]]
					mattype_element.set('desc',row[ind["Material Type"]])
					
				# Edit the Item Policy field if it exists, create it otherwise.
				# Note Item Policy has a code table included at the beginning
				# of this document.  The keys within are the publicly visible
				# descriptions, and the keys' values are what is needed to
				# update the code properly.
				if (ind["Item Policy"] > -1):
					itemtype_element = item_data.find('policy')
					if itemtype_element == None:
						# -> Field doesn't exists in data: add child to XML
						itemtype_element = ET.SubElement(item_data,'policy')
					itemtype_element.text = code_tables["Item Policy"][row[ind["Item Policy"]]]
					itemtype_element.set('desc',row[ind["Item Policy"]])
				
				# Edit the Enum A field if it exists, create it otherwise. ONLY if Enum A needs to be added.
				if (row[ind["Enum A"]] != None) and (row[ind["Enum A"]] != ''):
					enumA_element = item_data.find('enumeration_a')
					if enumA_element == None:
						# -> Field doesn't exists in data: add child to XML
						enumA_element = ET.SubElement(item_data,'enumeration_a')
					enumA_element.text = row[ind["Enum A"]]
					
				# Edit the Enum B field if it exists, create it otherwise. ONLY if Enum B needs to be added.
				if (row[ind["Enum B"]] != None) and (row[ind["Enum B"]] != ''):
					enumB_element = item_data.find('enumeration_b')
					if enumB_element == None:
						# -> Field doesn't exists in data: add child to XML
						enumB_element = ET.SubElement(item_data,'enumeration_b')
					enumB_element.text = row[ind["Enum B"]]
				
				# Edit the Chron I field if it exists, create it otherwise. ONLY if Chron I needs to be added.
				if (row[ind["Chron I"]] != None) and (row[ind["Chron I"]] != ''):
					chronI_element = item_data.find('chronology_i')
					if chronI_element == None:
						# -> Field doesn't exists in data: add child to XML
						chronI_element = ET.SubElement(item_data,'chronology_i')
					chronI_element.text = row[ind["Chron I"]]
				
				# Edit the Chron J field if it exists, create it otherwise. ONLY if Chron J needs to be added.
				if (row[ind["Chron J"]] != None) and (row[ind["Chron J"]] != ''):
					chronJ_element = item_data.find('chronology_j')
					if chronJ_element == None:
						# -> Field doesn't exists in data: add child to XML
						chronJ_element = ET.SubElement(item_data,'chronology_j')
					chronJ_element.text = row[ind["Chron J"]]
				
				# Format the output xml in a way Alma enjoys.
				output_xml = str(ET.tostring(root, encoding='utf-8'))[2:-1]
				
				# Make the second request pushing data into Alma.
				request2 = requests.put(updateUrl, params = {'apikey':apikey}, headers = {'Content-Type':'application/xml'}, data = output_xml)
				if (request2.status_code == 200):
					success_data.append(row)
				else:
					error_data.append(row)
					error_data[-1][ind["Notes"]] += ("; ","")[error_data[-1][ind["Notes"]] == ''] + "Err: #Problem with Networking request. Code " + str(request2.status_code)
					print("Item " + row[ind["Barcode"]] + ": Error. Code " + str(request2.status_code))
				
	
	_writeTo('suc_',success_data)
	_writeTo('err_',error_data)
	ts1 = time.time()
	print("Time to complete: " + str(round(ts1-ts0,2)) + " seconds")
	
"""_checkColumns(data, mand, opt, add)
		Verifies columns exist within a data set. The columns are classified as
		follows:
			1) The first set passed in to the function are mandatory columns. 
			   They must be present as they are necessary for the calling
			   function to work  and cannot be interpreted from other columns.
			   If any of them are not found, they will halt the program.
			2) The second set is for the optional columns. They should have 
			   their indices recorded if they are present, but otherwise should
			   get the index value of -1 and not be added to the data.
			3) The third set is the "add-in" columns.  These are necessary to the program but
			   can default to an empty string if they don't previously exist.
		It will return both the newly modified data and a column:index dict
""" 
def _checkColumns(data, mand, opt, add):

	# Input 'mand', 'opt', and 'add' are all ("ColName", "flags"[,'default'])
	#   tuples.
	# Output ind is a dict of indexes using the "colname": <index> format.
	# Output nums is a set of column names that need to be handled specially to 
	#   prevent CSV silliness (designated by a 'n' flag).
	# Output derived is a set of column names that won't prompt the user for input when updating.
	""" TODO: rename variables for increased understandability """
	ind = {}
	nums = set()
	der = set()
	
	opt_colnames = [col[0] for col in opt]
	add_colnames = [col[0] for col in add]
	
	# First check for columns that require other columns to function, and add
	# those found. Currently, this is only the "Description" field, and will
	# bring in the Enumeration and Chronology fields.

	if 'Description' in opt_colnames:
		if 'Enum A' not in opt_colnames:
			opt.append(('Enum A', 'tx'))
		if 'Enum B' not in opt_colnames:
			opt.append(('Enum B', 'tx'))
		if 'Chron I' not in opt_colnames:
			opt.append(('Chron I', 'tx'))
		if 'Chron J' not in opt_colnames:
			opt.append(('Chron J', 'tx'))
			
	if 'Description' in add_colnames:
		if 'Enum A' not in add_colnames:
			add.append(('Enum A', 'tx'))
		if 'Enum B' not in add_colnames:
			add.append(('Enum B', 'tx'))
		if 'Chron I' not in add_colnames:
			add.append(('Chron I', 'tx'))
		if 'Chron J' not in add_colnames:
			add.append(('Chron J', 'tx'))
	
	# Verify mandatory columns and fetch their locations. These will force close
	# the program if they are not present.
	for col in mand:
		if col[0] not in data[0]:
			print('Error: data must contain a "'+col[0]+'" column.')
			sys.exit(1)			
		ind[col[0]] = data[0].index(col[0])
		
		# Add the columns to the special formatting sets for easy access.
		if 'n' in col[1]:
			nums.add(col[0])
		if 'x' in col[1]:
			der.add(col[0])
		
	# Record the presence of these columns if they exist, but do not add them if
	# they aren't present. These are for checks and tests that may not work if 
	# the list is unformatted
	for col in opt:
		if col[0] in data[0]:
			ind[col[0]] = data[0].index(col[0])
			
			# Add the columns to the special formatting sets for easy access.
			if 'n' in col[1]:
				nums.add(col[0])
			if 'x' in col[1]:
				der.add(col[0])
			
	# Add in these columns that may/may not be present
	for col in add:
		if col[0] not in data[0]:
			# Append a new column and add the header
			for row in data:
				row.append("")
			data[0][-1] = col[0]
		ind[col[0]] = data[0].index(col[0])
		
		# Add the columns to the special formatting sets for easy access.
		if 'n' in col[1]:
			nums.add(col[0])
		if 'x' in col[1]:
			der.add(col[0])
	
	# Return both the data and the index set.
	return (data, ind, nums, der)
					
def _checkValue(colname, value, msg):
	while (value not in code_tables[colname]):
		# Construct & display an error message
		message = "Value '" + value + "' for column '" + colname + "' is not possible. Possible options are: "
		for option in code_tables[colname]:
			message += "'" + option + "', "
		message = message[:-2] + ".\nIf you need a different value the program doesn't recognize, cancel with Ctrl+C and add the value to the program's code table.\n"
		print(message)
		
		value = str(input(msg))
		print('\n')
	
	# Out of loop means a valid value was found
	return value
	
def _matchDescriptions(data, ind):

	# This is the list of patterns used when processing descriptions.  Use a
	# (name, pattern) tuple when adding to the list.  Order matters - keep
	# the broadest patterns at the beginning of the list and append more
	# specific, irregular, or uncommon patterns to the end.
	""" TODO: Determine what to do when primary enumeration is year instead of
			volume
		TODO: Add patterns for exact date descriptions. 
		TODO: Add optional pre-volume capturing group within volume information
			somehow (e.g. ser3 v2 ...)
	"""
	descPatternStrings = [("StdMatch",'^\s*(?P<enumAType>(?:SER\.?\s*\d+\s*)?VO?L?\s*[\.:]?\s?)\s*(?P<enumANum>\d+[-/]?\d*)\s*(?P<enumB>(?:(?:\s+NO?S?|\s+P[PTG]?)\s*\.?\s*\d+[-/]?\d*)*)\s*(?:\(?\s*(?P<chronJ>(?:(?:JAN?[A-Z]*|FE[A-Z]*|MA?R[CH]*|AP[RIL]*|MA?Y|JU?[NE]E?|JU?[LY]Y?|AU?G[UST]*|SE[PTEMBR]*|O[A-Z]*|NO?V[A-Z]*|D[A-Z]*|SP[RING]*|SU[MER]*|AUT[UMN]*|FA[L]*|W[A-Z]*)\.?\s*[-/]?\s*){0,2})\s*(?P<chronI>(?<!\d)\d{2,4}(?:[-/]\d{1,4})?)\s*\)?)?[ \t]*$'),
	
	("YearBeforeMonth",'^\s*(?P<enumAType>(?:SER\.?\s*\d+\s*)?VO?L?\s*[\.:]?\s?)\s*(?P<enumANum>\d+[-/]?\d*)\s*(?P<enumB>(?:(?:\s+NO?S?|\s+P[PTG]?)\s*\.?\s*\d+[-/]?\d*)*)\s*\(?\s*(?P<chronI>(?<!\d)\d{4}(?:[-/]\d{1,4})?)\s*(?P<chronJ>(?:(?:JAN?[A-Z]*|FE[A-Z]*|MA?R[CH]*|AP[RIL]*|MA?Y|JU?[NE]E?|JU?[LY]Y?|AU?G[UST]*|SE[PTEMBR]*|O[A-Z]*|NO?V[A-Z]*|D[A-Z]*|SP[RING]*|SU[MER]*|AUT[UMN]*|FA[L]*|W[A-Z]*)\.?\s*[-/]?\s*){1,2})\s*\)?[ \t]*$'),
	
	("SplitYears",'^\s*(?P<enumAType>(?:SER\.?\s*\d+\s*)?VO?L?\s*[\.:]?\s?)\s*(?P<enumANum>\d+[-/]?\d*)\s*(?P<enumB>(?:(?:\s+NO?S?|\s+P[PTG]?)\s*\.?\s*\d+[-/]?\d*)*)\s*\(?\s*(?P<chronJpt1>JAN?[A-Z]*|FE[A-Z]*|MA?R[CH]*|AP[RIL]*|MA?Y|JU?[NE]E?|JU?[LY]Y?|AU?G[UST]*|SE[PTEMBR]*|O[A-Z]*|NO?V[A-Z]*|D[A-Z]*|SP[RING]*|SU[MER]*|AUT[UMN]*|FA[L]*|W[A-Z]*)\s*(?P<chronIpt1>(?<!\d)\d{2,4})\s*[-/]\s*(?P<chronJpt2>JAN?[A-Z]*|FE[A-Z]*|MA?R[CH]*|AP[RIL]*|MA?Y|JU?[NE]E?|JU?[LY]Y?|AU?G[UST]*|SE[PTEMBR]*|O[A-Z]*|NO?V[A-Z]*|D[A-Z]*|SP[RING]*|SU[MER]*|AUT[UMN]*|FA[L]*|W[A-Z]*)\s*(?P<chronIpt2>(?<!\d)\d{2,4})\s*\)?[ \t]*$')]
	
	# Pattern calls made against this precompiled list of patterns rather
	# than the strings above.
	p = [(i[0],re.compile(i[1], flags=re.I)) for i in descPatternStrings]

	# Test the description of each item sequentially against the regex
	# pattern list.  Record the name of the matched pattern, unless none is
	# found 
	no_match_count = 0
	for row in data[1:]:
		for i in p:
			result = i[1].match(row[ind["Description"]])
			if result != None:
				# Match found, record the groups of information and the
				# pattern that matched. Check each for existence and replace
				# with empty string if not found.
				"""TODO: Handle this assignment more elegantly."""
				
				named_groups = result.groupdict()
				
				#Enum A
				if 'enumAType' in named_groups and result.group('enumAType') != None:
					row[ind["Enum A"]] = result.group('enumAType')
				if 'enumANum' in named_groups and result.group('enumANum') != None:
					row[ind["Enum A"]] += result.group('enumANum')
				#Enum B
				if 'enumB' in named_groups and result.group('enumB') != None:
					row[ind["Enum B"]] = result.group('enumB').strip()
				#Chron I
				if 'chronI' in named_groups and result.group('chronI') != None:
					row[ind["Chron I"]] = result.group('chronI')
				else:
					if 'chronIpt1' in named_groups and result.group('chronIpt1') != None:
						row[ind["Chron I"]] = result.group('chronIpt1')
					if 'chronIpt2' in named_groups and result.group('chronIpt2') != None:
						row[ind["Chron I"]] += "-" + result.group('chronIpt2')
				#Chron J
				if 'chronJ' in named_groups and result.group('chronJ') != None:
					row[ind["Chron J"]] = result.group('chronJ')
				else:
					if 'chronJpt1' in named_groups and result.group('chronJpt1') != None:
						row[ind["Chron J"]] = result.group('chronJpt1')
					if 'chronJpt2' in named_groups and result.group('chronJpt2') != None:
						row[ind["Chron J"]] += "-" + result.group('chronJpt2')
				#Pattern	
				row[ind["Pattern"]] = i[0]
				break
				
		else: # -> No match was found using the list of patterns.
				row[ind["Pattern"]] = "N/A"
				no_match_count += 1
	
	# Alert user of the number of non-matching/errors found.
	if no_match_count == 0:
		print("All item descriptions parsed successfully")
	else:
		print ("Could not parse " + str(no_match_count) + " item description" +
		("","s")[no_match_count > 1])
	
	return data

def _readFile(filename):
	
	try:
		file = open(filename,"r")
	except IOError:
		print("file not found")
		sys.exit(1)
		
	data = []
	# Read the file into useable data
	for eachline in file:
		eachline = eachline.strip() #remove the newline character from the end
		a = eachline.split(",")
		data.append(a)
	
	# Done reading file, close it.
	file.close()
	
	return data
		
def _writeTo(prefix, data):
	
	# Remove previous prefixes from the filename to prevent prefix buildup over
	# multiple iterations.
	old_filename = sys.argv[1]
	if ('f_' == old_filename[:2]) or ('s_' == old_filename[:2]):
		old_filename = old_filename[2:]
	elif('err_' == old_filename[:2]) or ('suc_' == old_filename[:2]):
		old_filename = old_filename[4:]
	new_filename = prefix + old_filename
	output_file = open(new_filename, 'w')
	
	# Iterate over the data, left to right, top to bottom, splitting 
	# values with commas in the output string.
	for row in data:
		output_string = ""
		for item in row:
			output_string += item + ","
		output_string = output_string[:-1] #Remove final comma
		output_string += '\n'
		output_file.write(output_string)
	
	output_file.close()
	
	# Choose an appropriate message to display depending on the input
	# prefix.
	if prefix == 'f_':
		message = "Formatted data written to file "
	elif prefix == 's_':
		message = "Pattern-matched data written to file "
	else:
		message = "File written at "
		
	print(message + new_filename + "\n")
	return new_filename
	
if __name__ == "__main__":
	main()
