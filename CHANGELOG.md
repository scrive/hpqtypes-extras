# hpqtypes-extra-1.10.3.1 (2021-01-26)
Generate valid INSERT SELECT query with data modifying WITH clauses

# hpqtypes-extra-1.10.3.0 (2020-11-16)
* Include LIMIT clause in UNION subqueries of the select

# hpqtypes-extras-1.10.2.1 (2020-05-05)
* Add support for GHC 8.10

# hpqtypes-extras-1.10.2.0 (2020-01-20)
* Add support for UNION clause
* Add support for GHC 8.8

# hpqtypes-extras-1.10.1.0 (2020-01-09)
* Add support for ON CONFLICT clause

# hpqtypes-extras-1.10.0.0 (2019-11-05)
* Implement `UuidT` Column Type ([#28](https://github.com/scrive/hpqtypes-extras/pull/28)).
* Fix sqlValidateCheck and sqlValidateFK

# hpqtypes-extras-1.9.0.1 (2019-06-04)
* Create composite types automatically only if database is empty
  ([#24](https://github.com/scrive/hpqtypes-extras/pull/24)).

# hpqtypes-extras-1.9.0.0 (2019-05-22)
* Extend `checkDatabaseAllowUnknownTables` to allow unknown composite
  types and rename it to `checkDatabaseAllowUnknownObjects`
  ([#22](https://github.com/scrive/hpqtypes-extras/pull/22)).
* Remove the `Default` instance for `ExtrasOptions`; use
  `defaultExtrasOptions` instead
  ([#23](https://github.com/scrive/hpqtypes-extras/pull/23)).

# hpqtypes-extras-1.8.0.0 (2019-04-30)
* Make composite types subject to migration process
  ([#21](https://github.com/scrive/hpqtypes-extras/pull/21)).
* Add a migration type for concurrent creation of an index
  ([#21](https://github.com/scrive/hpqtypes-extras/pull/21)).

# hpqtypes-extras-1.7.1.0 (2019-02-04)
* Fix an issue where unnecessary migrations were run sometimes
  ([#18](https://github.com/scrive/hpqtypes-extras/pull/18)).

# hpqtypes-extras-1.7.0.0 (2019-01-08)
* Added support for no-downtime migrations
  ([#17](https://github.com/scrive/hpqtypes-extras/pull/17)):
    - `sqlCreateIndex` is deprecated. Use either
      `sqlCreateIndexSequentially` or `sqlCreateIndexConcurrently`
      (no-downtime migration variant) instead.
    - `sqlAddFK` is deprecated. Use either `sqlAddValidFK` or
      `sqlAddNotValidFK` (no-downtime migration variant) instead.
    - API addition: `sqlValidateFK`, for validating a foreign key
      previously added with `sqlAddNotValidFK`.
    - `sqlAddCheck` is deprecated. Use either `sqlAddValidCheck` or
      `sqlAddNotValidCheck` (no-downtime migration variant) instead.
    - API addition: `sqlValidateCheck`, for validating a check
      previously added with `sqlAddNotValidCheck`.
    - API addition: `sqlAddPKUsing`, converts a unique index to a
      primary key.
    - New `Table` field: `tblAcceptedDbVersions`.
* `ValidationResult` is now an abstract type.
* `ValidationResult` now supports info-level messages in addition to errors.

# hpqtypes-extras-1.6.4.0 (2019-02-04)
* Fix an issue where unnecessary migrations were run sometimes
  ([#19](https://github.com/scrive/hpqtypes-extras/pull/19)).

# hpqtypes-extras-1.6.3.0 (2018-11-19)
* API addition: `sqlWhereAnyE`
  ([#16](https://github.com/scrive/hpqtypes-extras/pull/16)).

# hpqtypes-extras-1.6.2.0 (2018-07-11)
* Support hpqtypes-1.6.0.0.
* Drop support for GHC < 8.

# hpqtypes-extras-1.6.1.0 (2018-03-18)
* Add support for GHC 8.4.
* Drop support for GHC 7.8.

# hpqtypes-extras-1.6.0.0 (2018-01-25)
* Introduce `checkPKPresence` to enforce primary keys on all tables supplied to `checkDatabase`
* Introduce an options data type, `ExtrasOptions`

# hpqtypes-extras-1.5.0.1 (2018-01-09)
* Changed `getDBTableNames` to only schemas explicitly in search path, rather
  than an exclusion list. Affects table version and unknown tables checks.

# hpqtypes-extras-1.5.0.0 (2017-12-08)
* Changed internal representation of PrimaryKey to NubList (#11)
  This will break existing PKs set on multiple columns unless they are
  alphabetically sorted in the defining list.

# hpqtypes-extras-1.4.0.0 (2017-11-24)
* Introduced tsvector postgres type and indexing methods GIN and BTree

# hpqtypes-extras-1.3.1.1 (2017-07-21)
* Now depends on 'log-base' instead of 'log'.

# hpqtypes-extras-1.3.1.0 (2017-07-20)
* Improved migration order sanity checking (#7).

# hpqtypes-extras-1.3.0.0 (2017-05-17)
* Add drop table migrations.
* Add a test suite.
* Improve documentation.
* Add option to force commit after every migration.

# hpqtypes-extras-1.2.4 (2016-07-28)
* Initial release.
