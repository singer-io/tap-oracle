# Changelog

## 0.3.1
 * Adds handling for columns that do not have a datatype -- those columns will have `inclusion`=`unavailable` and `sql-datatype`=`"None"` [#19](https://github.com/singer-io/tap-oracle/pull/19)

## 0.3.0
 * Adds optional parameter `scn_window_size` to allow for an scn window during logminer replication [#18](https://github.com/singer-io/tap-oracle/pull/18)
