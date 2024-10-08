# Changelog

## 1.3.0
 * Set utf-8 encoding while connecting to the oracle DB and load the non-ascii characters to target (if any) [#41](https://github.com/singer-io/tap-oracle/pull/41)

## 1.2.1
 * Fixes an issue where incremental limit queries would not use the latest replication_key_value from state [#39](https://github.com/singer-io/tap-oracle/pull/39)

## 1.2.0
 * Adds rownum queries to be used with incremental_limit config value [#35](https://github.com/singer-io/tap-oracle/pull/35)

## 1.1.2
 * Log value of mine_sql [#30](https://github.com/singer-io/tap-oracle/pull/30)

## 1.1.1
 * Set a maximum length on Singer Decimals, where a decimal past the cap is normalized via `decimal.normalize()` [#28](https://github.com/singer-io/tap-oracle/pull/28)

## 1.1.0
 * Values with Decimal precision will now be written as strings with a custom `singer.decimal` format in order to maintain that precision through the pipeline [#26](https://github.com/singer-io/tap-oracle/pull/26)

## 1.0.1
 * Increase default numeric scale from `6` to `38` [#24](https://github.com/singer-io/tap-oracle/pull/24)

## 1.0.0
 * Backwards incompatible change to the way that data types are discovered and parsed [#22](https://github.com/singer-io/tap-oracle/pull/22)
   * Oracle numeric types with a null scale (`NUMBER` and `NUMBER(*)`) will now be correctly discovered as floating point types rather than integers.
   * This may cause downstream issues with loading and reporting, so a major bump is required.

## 0.3.1
 * Adds handling for columns that do not have a datatype -- those columns will have `inclusion`=`unavailable` and `sql-datatype`=`"None"` [#19](https://github.com/singer-io/tap-oracle/pull/19)

## 0.3.0
 * Adds optional parameter `scn_window_size` to allow for an scn window during logminer replication [#18](https://github.com/singer-io/tap-oracle/pull/18)
