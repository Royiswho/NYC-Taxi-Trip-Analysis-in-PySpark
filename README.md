# NYC-Taxi-Trip-Analysis-in-PySpark

This is a PySpark project which analyzes the an open-source New York city taxi trip dataset.
The dataset was released under the FOIL (The Freedom of Information Law) and made public
by Chris Whong (https://chriswhong.com/open-data/foil_nyc_taxi/).

#### Author: Yi Rong
#### Date: 02/17/2021

---

## Description

* main_task1.py: Top-10 Active Taxis
* main_task2.py: Top-10 Best Drivers 
* main_task3.py: Best time of the daty to Work on Taxi

# Other Documents

* OUTPUT1: Task 1 large data results 
* OUTPUT2: Task 2 large data results 
* OUTPUT3: Task 3 large data results 

# How to run  

Go to the directory where places dataset and .py files. Run the task 1, 2, 3 by submitting the task to spark-submit. 

Task1
```python

spark-submit main_task1.py taxi-data-sorted-small.csv.bz OUTPUT1

```


Task2
```python

spark-submit main_task2.py taxi-data-sorted-small.csv.bz OUTPUT2

```


Task3
```python

spark-submit main_task3.py taxi-data-sorted-small.csv.bz OUTPUT3

```



