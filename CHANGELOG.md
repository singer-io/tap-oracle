# Changelog

## 1.0.0
 * Backwards incompatible change to the way that data types are discovered and parsed [#22](https://github.com/singer-io/tap-oracle/pull/22)
   * Oracle numeric types with a null scale (`NUMBER` and `NUMBER(*)`) will now be correctly discovered as floating point types rather than integers.
   * This may cause downstream issues with loading and reporting, so a major bump is required.

## 0.3.1
 * Adds handling for columns that do not have a datatype -- those columns will have `inclusion`=`unavailable` and `sql-datatype`=`"None"` [#19](https://github.com/singer-io/tap-oracle/pull/19)

## 0.3.0
 * Adds optional parameter `scn_window_size` to allow for an scn window during logminer replication [#18](https://github.com/singer-io/tap-oracle/pull/18)
