last 3 csv files were the output formats of the python program
and all the numeric csv files there are the satellite data , create a folder with name 'satellite data readings' and place the satellite data inside it - the folder is in base directory 
place all_files_years_and_averages.csv file also in the base directory 
place cf.py file also in a base directory 
RUN the progam 
✅

The cf.py script calculates correction factors between observed and satellite precipitation data for various weather stations.
Here's what it does:
Calculates Satellite Averages: It reads precipitation data from multiple satellite files (CSV/Excel) in the satellite data readings subdirectory, calculates the yearly average precipitation for each station, and saves these averages to satellite_yearly_averages.csv.
Reads Observed Data: It reads an observed precipitation data file (all_files_years_and_averages.csv or .xlsx) from the base directory. It extracts station numbers and yearly average observed precipitation.
Calculates Yearly Correction Factors: It merges the observed and satellite yearly average data based on matching station numbers and years. Then, it calculates a "Correction Factor" for each station-year pair by dividing the "Observed Average" by the "Satellite Average". These yearly correction factors are saved to yearly_correction_factors.csv.
Calculates Grand Correction Factors: Finally, it computes a "Grand Correction Factor" for each station by taking the average of all valid yearly correction factors for that station. These grand correction factors are saved to grand_correction_factors.csv.
Base Directory: The script expects to be run from C:\Users\aaa\Desktop\correction factor of every station.
Input Directory: Satellite data files are read from C:\Users\aaa\Desktop\correction factor of every station\satellite data readings.
Output Directories:
Processed satellite yearly averages are saved to C:\Users\aaa\Desktop\correction factor of every station\satellite_yearly_averages.csv.
Yearly correction factors are saved to C:\Users\aaa\Desktop\correction factor of every station\yearly_correction_factors.csv.
Grand correction factors are saved to C:\Users\aaa\Desktop\correction factor of every station\grand_correction_factors.csv.
The observed data file (all_files_years_and_averages.csv or .xlsx) is also expected in the base directory.





