# GenexPlus

This project is a general exploration tool for time series. It implements Dynamic Time Warping (DTW) and pre-processing by clustering discussed in these papers.

http://real.mtak.hu/74287/1/p1595_neamtu_u.pdf
http://real.mtak.hu/43722/1/p169_neamtu_u.pdf
https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=8509275

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites
The project uses Python interpreter version 3.6, but any latest Python version should work.

Make sure the following packages is installed in your environment:

cycler==0.10.0

fastdtw==0.3.2

kiwisolver==1.0.1

matplotlib==3.0.3

numpy==1.16.2

pandas==0.24.2

py4j==0.10.7

pyparsing==2.4.0

pyspark==2.4.1

python-dateutil==2.8.0

pytz==2018.9

scipy==1.2.1

six==1.12.0

sqlparse==0.3.0

psutil==5.6.2

You can install the package through pip install 

```
pip install numpy
```

## To Start Running the program
Navigate to the project's root directory. Run the following command:
```
python3 CLI_refactor.py
```
Now you are in the Genex Console, you should see the following prompt:
```
Java Home Path is set to None
GenexPlus > 
```
Because PySpark runs Java, you need to set the Java Home for Genex. The Java version below 9.0 and above 1.7 is requried for the current implementation. 

While in Genex Console, use the following command to set Java Home:
```
set <JAVA_HOME> 
```
For example:
```
set /Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home
```
If the above command runs successfully, you should see the following message in the console:
```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Java home set at /Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home
```
If you see warnings in the message, you could ignore them for they won't affect the program's functionality.

## GenexPlus Project
GenexPlus organizes user content under projects. To start, use the following command to open or create a GenexPlus Project:
```
open <project_name>
```
For example:
```
open example_project
```
If the project of given name is not found in Genex directory, you will be prompted if you wish to create a new project.

# Variables and Data Structures
## ts_list
key-value pairs of all the time series
####Structure
list of time series id and data in key-value pair form. Each entry is a list consisted of [time series id, 
time series data], where time series id is a string, time series data is a list of numeric values.
####Initialization and Usage 
This variable is set in the load operation found in CLI.py.
## norm_ts_list
key-value pairs of all the time series with the data normalized
####Structure
norm_ts_list has the exact same structure as the ts_list but with the value in 'time series data' normalized using
minmax normalization across all the entries in ts_list
#### Initialization and Usage 
Like ts_list, norm_ts_list is also set in the load operation in CLI.py. It is created by this method 'normalize_ts_with_min_max'
In addition, norm_ts_list is broadcasted before the cluster operation because it is used in the cluster map function.
## group_rdd
temperory variable that caches the spark operation
####Structure
If collected, group_rdd should be a list of key-value pairs. The key is the length of a sequence. The value is a list
containing sequences of that length. For the representation of a sequence, see (sequence or subsequence) under 'Terms'.
## cluster_rdd
####Structure
If collected ...
# Terms
#### sequence or subsequence
a subset of a time series. A sequence is usually represented with three key values: id, start and end, where id tells
from which time series was the sequence extracted; start and end denotes on which point was the sequence from.
For example:
```
times series id, time series data
id1, [1,2,3,4,5]
id2, [5,4,3,2,1]
id3, [6,7,8,9,0,3]
```
A sequence denoted by ['id2', 2, 4] gives raw data: [3,2,1]
 
# Functions
...