# Data Engineering Assignment : Small File Problem

## Introduction
When interviewing a data-minded candidate and potential future colleague of ours, we believe that an interview based on a handed-out task is often more open, more relevant and more interesting for both parties.

It is important to emphasize that the task at hand should not be interpreted as having a right or wrong answer, but merely acts as a basis for a discussion, and a way for us to get a glimpse of how you as a candidate choose to go about it. It is not a requirement to implement all parts of the specified tasks - use it as a guidance and implement the solution the way you feel is right.

## The Objective
The assignment focuses on solving a variant of the small-files problem. 
You are given access to a GCP bucket with many small .csv files that have various data structures in them. 
Your goal is to perform a series of ETL steps that demonstrate your ability to work with simple data. It would be preferred if you use Python for you solution, but you can use whatever language you decide for :) You can use Scala, Python or R to solve the problem.

## Access
The generated data files needed for the assignment is located in the public storage bucket: <https://storage.googleapis.com/de-assignment-data-bucket/data/>.

To access the storage bucket, we recommend [gsutil](https://cloud.google.com/storage/docs/gsutil) to navigate in storage buckets on Google Cloud Platform.
Alternatively, you can curl the storage bucket to acquire the tree structure and URI's using:
```
curl -X GET "https://storage.googleapis.com/storage/v1/b/de-assignment-data-bucket/o"
```
Files can then be downloaded by using wget. I.e:
```
wget https://storage.googleapis.com/de-assignment-data-bucket/data/<FILE>
```

Note, that most programming languages have libraries to interact with GCP. E.g. the python library can be found [here](https://github.com/googleapis/google-cloud-python#google-cloud-python-client).

Hint: Since you will call the bucket without a GCP project, you would like to use an [anonymous client](https://googleapis.dev/python/storage/latest/client.html#google.cloud.storage.client.Client.create_anonymous_client).

## Input
You are given input files with the naming convention : `<craft>_<planet>_<date>_<time>.csv` :

`<craft>`: [`rocket`, `lander`] <br>
`<planet>` : [`venus`, `saturn`] <br>
`<date>_<time>` has the following format : `yyyyMMdd_HHmmss`

There is four different file formats :

| rocket_venus       | rocket_saturn       | lander_venus       | lander_saturn    | 
| ------------------ | ------------------- | ------------------ | ---------------- |
| id (UUID)          | id (UUID)           | id (UUID)          | id (UUID)        |
| size (String)      | size (String)       | size (String)      | size (String)    |
| speed (Float)      | Mass (Float)        | coRe (Float)       | core (Float)     |
| axis_ANGLE (Float) | gravity (Float)     | suspension (Float) | SPEED (Float)    |
|                    | temperature (Float) | thrust (Float)     | force (Float)    |
|                    | life (Boolean)      | weight (Float)     | clones (Integer) |
|                    |                     | crew (Integer)     |                  |

## Tasks
1. ##### Data ingestion:
 - Fetch files from the Google Cloud Platform bucket in batches of a certain size.
2. ##### Data transformation:
 - Extract the `<date>_<time>` components from each file name, and convert that to a timestamp. Add the timestamp as a column called `timestamp` in the given file in the format `yyyy-MM-dd HH:mm:ss`
 - Parse the `id` column's middle value (for `bf8d460f-943c-4084-835c-a03dde141041` this is `4084`), and use that as an id in the newly generated file.
 - Convert all column names to lowercase
 - From the `size` column :
    - filter out all non-integer values and create a new column called `size` of type Integer
    - based on the value of the newly created integer-based `size` column, create a new column called `magnitude` that is of the type String. Populate the `magnitude` column by mapping the `size` values to their respective range according to the following scheme:
        - `massive` : 500 <= x < 1000
        - `big` : 100 <= x < 500
        - `medium` : 50 <= x < 100
        - `small` : 10 <= x < 50
        - `tiny` : 1 <= x < 10
    - drop the original `size` column
 - Output a single csv file per craft per planet (ex. `rocket_venus.csv`)
3. ##### Testing:
 - Write test cases that proves the correctness of your solution.

## Delivery
Please provide your solution in the form of a link to a GitHub repository hosting your source code.

## General Remarks
As much as this is a coding exercise, your goal is to show not only _what_ your work looks like but also how you work. Hence, a successful delivery doesn't only focus on the code written.

Other aspects, we deem quite important, are:
- Strong commit history
- Modular design
- Testing (unit testing, integration testing etc.)
- Documentation

If you get stuck or don't understand something, please reflect that in the documentation and move on.
Any assumptions or considerations that you are making need to be stated.
In general, your thought process what matters to us, so please make it apparent.

##  Good luck!



