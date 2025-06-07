-- Create database
--example [YOUR_DATABASE]='CORTEX_ANALYST_LOCAL'
CREATE DATABASE IF NOT EXISTS [YOUR_DATABASE];
USE DATABASE [YOUR_DATABASE]

-- Create schema
--CREATE OR REPLACE SCHEMA cortex_analyst_demo.revenue_timeseries;→CREATE OR REPLACE SCHEMA [YOUR_DATABASE].cortex_analyst;
CREATE OR REPLACE SCHEMA [YOUR_DATABASE].cortex_analyst;

-- Create stage for raw data
CREATE OR REPLACE STAGE raw_data DIRECTORY = (ENABLE = TRUE);


