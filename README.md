## Redshift DDL Benchmark Comparison

The goal of this repo will be to quickly run performance tests that can compare different sort, distribution, encoding styles on Redshift tables.

Will add more documentation in future. Example configuration in config.json.

#### Configuration

Use a JSON file to configure things like your tests groups, set-up and tear-down procedures, number of runs, etc. Example configuration is in config.json.

The following keys are available to set (along with nested params and descriptions):

```
1. groups (array<object>): List of test groups
    a. name (string): Name of test group
    b. description (string): Brief description of the group for personal reference
    c. isControl (bool): Whether or not the 

2. setUp (array<string>): List of SQL queries to run on the DB before any benchmark tests

3. queries ():

4. teardown (array<string>): List of SQL queries to run after all tests complete (or potentially fail)
```

#### TODO:

*Critical:*
- [ ] Sortkey order is important
- [ ] VACUUM/ANALYZE before running queries
- [ ] Table metrics compared with control (tables.csv)
- [ ] Primary/Foreign keys (and identity columns?)
- [ ] ANALYZE COMPRESSION used for encoding (set as config table mod flag?)

*Trivial/Non-Critical:*
- [ ] expectedResult property checked for queries
- [ ] Gzip output (report.csv, tables.csv, error.log, run.log)
- [ ] Handle exceptions properly (rollback, cleanup, log)
