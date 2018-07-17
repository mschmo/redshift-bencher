## Redshift DDL Benchmark Comparison

The goal of this repo will be to quickly run performance tests that can compare different sort, distribution, encoding styles on Redshift tables.

Will add more documentation in future. Example configuration in config.json.

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
