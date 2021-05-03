# NYC-Taxi-Trip-Analysis-in-PySpark

This is a PySpark project which analyzes the an open-source New York city taxi trip dataset.
The dataset was released under the FOIL (The Freedom of Information Law) and made public
by Chris Whong (https://chriswhong.com/open-data/foil_nyc_taxi/).

#### Author: Yi Rong

#### Date: 02/17/2021

---

## Data Description

|     | Attribute          | Description                                                       |
|-----|--------------------|-------------------------------------------------------------------|
| 0   | medallion          | an md5sum of the identifier of the taxi - vehicle bound (Taxi ID) |
| 1   | hack license       | an md5sum of the identifier for the taxi license (Driver ID)      |
| 2   | pickup datetime    | time when the passenger(s) were picked up                         |
| 3   | dropoff datetime   | time when the passenger(s) were dropped off                       |
| 4   | trip time in secs  | duration of the trip                                              |
| 5   | trip distance      | trip distance in miles                                            |
| 6   | pickup longitude   | longitude coordinate of the pickup location                       |
| 7   | pickup latitude    | latitude coordinate of the pickup location                        |
| 8   | dropoff longitude  | longitude coordinate of the drop-off location                     |
| 9   | dropoff latitude   | latitude coordinate of the drop-off location                      |
| 10  | payment type       | the payment method -credit card or cash                           |
| 11  | fare amount        | fare amount in dollars                                            |
| 12  | surcharge          | surcharge in dollars                                              |
| 13  | mta tax            | tax in dollars                                                    |
| 14  | tip amount         | tip in dollars                                                    |
| 15  | tolls amount       | bridge and tunnel tolls in dollars                                |
| 16  | total amount       | total paid amount in dollars                                      |

# Code Description

* main_task1.py: Top-10 Active Taxis
* main_task2.py: Top-10 Best Drivers 
* main_task3.py: Best time of the daty to Work on Taxi

# Other Documents

* docs/OUTPUT1: Task 1 large data results 
* docs/OUTPUT2: Task 2 large data results 
* docs/OUTPUT3: Task 3 large data results 

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



