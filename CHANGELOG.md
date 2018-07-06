# hpqtypes-extras-1.6.2.0 (2018-??-??)
* Support hpqtypes-1.6.0.0.
* Drop support for GHC < 8.

# hpqtypes-extras-1.6.1.0 (2018-03-18)
* Add support for GHC 8.4.1.
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
