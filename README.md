# Frontify hiring process task
Little project for Frontify interview process

## Context
Check the DATA_ANALYTICS_ENGINEER_TASKS.pdf for more details about the task

## Methodology
I opted for doing all the data manipulation direclty on Snowflake via SQL, since dealing with structured data and no ML or heavy math was required

Exported the data as CSV in order to do the visualisation

I used Jupyter Notebooks to generate a single HTML file with the code and visualisations

There is a .sh file with the record of some commands used to work with Git and virtual environments (useful for reproducibility)

I used Visual Studio as my IDE

## Data
Snowflake environment can be found [here](https://frontify.eu-central-1.snowflakecomputing.com/console/login#/) and the credentials can be found at the email received when scheduling the interview

The raw data was loaded [here](./data/) as CSVs. There is also a CSV for deal_fact excluding values that do not fit the deal pipeline definition

Description of data points can be found [here](https://docs.google.com/spreadsheets/d/e/2PACX-1vQOXtMbdYoeXK6RyzW9tsopGb38Nd7zHAHJo26vOstIc6E9jjUxRdiKEwE_VgGmR84-FZln4c6EXGpN/pubhtml)