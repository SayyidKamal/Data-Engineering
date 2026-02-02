import pandas as pd # for processing data 
import sys #for sys module to use
sys.argv # to access command-line arguments
#print ('Arguments:', sys.argv) # to print all arguments

month = int(sys.argv[1]) # to get the first argument as an integer

print (f'hello from pipeline.py! The month is {month}')



df = pd.DataFrame({'day': [1, 2], 'num_passengers': [3, 4]}) # create a simple DataFrame
df['month'] = month 
print(df.head()) # print the first few rows of the DataFrame
df.to_parquet(f'output_{month}.parquet') # save the DataFrame to a parquet file