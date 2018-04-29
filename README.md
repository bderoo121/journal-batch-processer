# Item Record Batch Processor
This script is designed for the Alma library management system.  It takes CSV files of item records and runs up to three different processes on the contents.

1. Formatting the CSV file (-f).
2. Splitting the description field into relevant parts (-s). This also overwrites fields selectively or completely depending on the settings in the python file (See configuration) 
3. Updating the library system with the CSV contents (-u).  

This script is run from the command line, as in BatchUpdate.py ItemRecords.csv[ --f][ --s][ --u]. Including multiple flags will run them, in order, --f -> --s -> --u.
