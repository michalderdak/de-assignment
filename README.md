# Data Engineering Assignment : Small File Problem

## Introduction
When interviewing a data-minded candidate and potential future colleague of ours, we believe that an interview based on a handed-out task is often more open, more relevant and more interesting for both parties.

It is important to emphasize that the task at hand should not be interpreted as having a right or wrong answer, but merely acts as a basis for a discussion and a way for us to get a glimpse of how you as a candidate choose to go about it. It is not a requirement to implement all parts of the specified tasks - use it as a guidance and implement the solution the way you feel is right.

## The Objective
The assignment focuses on solving a variant of the small files problem. 
You are given access to a S3 bucket with many small csv files that have various data structures in them. 
Your goal is to perform a series of ETL steps that demonstrate your ability to work with simple data. It would be preferred if you use Python for you solution, but you can use whatever language you decide for :) You can use Scala, Python or R to solve the problem.

## Access
The generated data files needed for the assignment is located in the [data folder](https://github.com/lunarway/de-assignment/tree/initial_datafiles_readme/data) of this repos

## Input
You are given input files with the naming convention : `<craft>_<planet>_<date>_<time>.csv` :

`<craft>` : [`rocket`, `lander`] <br>
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
1. Download files from Github in batches of a certain size.
2. Extract the `<date>_<time>` componanats from each file name, and convert that to a timestamp. Add teh timestamp as a column called `timestamp` in the given file in the format `yyy-MM-dd HH:mm:ss`
3. Parse the `id` column's middle value (for `bf8d460f-943c-4084-835c-a03dde141041` this is `4084`), and use that as an id in the newly generated file.
4. Convert all columnnames to lowercase
5. From the `size` column :
    - filter out all non-integer values and create a new column called `size` of type Integer
    - based on the value of the newly created integer-based `size` column, create a new column called `magnitude` that is of the type String. Populate the `magnitude` column by mapping the `size` values to thier respective range according to the following scheme :
        - `massive` : 500 <= x < 1000
        - `big` : 100 <= x < 500
        - `medium` : 50 <= x < 100
        - `small` : 10 <= x < 50
        - `tiny` : 1 <= x < 10
    - drop the original `size` column
6. Output a single csv file per craft per planet (ex. `rocket_venus.csv`)

## Delivery
Please provide your solution in the form of a link to a Github repository hosting your source code.

## General Remarks
As much as this is a coding exercise, your goal is to show not only _what_ your work looks like but also how you work. Hence, a successful delivery doesn't only focus on the code written.

Other aspects, we deem quite important, are:
- strong commit history
- modular design
- unit tests
- documentation

If you get stuck or don't understand something, please reflect that in the documentation and move on.
Any assumptions or considerations that you are making need to be stated.
In general, your thought process what matters to us, so please make it apparent.

##  Good luck!




