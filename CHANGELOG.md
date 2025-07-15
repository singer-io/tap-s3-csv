# Changelog

## 1.5.1
  * Bump dependency versions for twistlock compliance [#72](https://github.com/singer-io/tap-s3-csv/pull/72)

## 1.5.0
  * Changes JSONFileCache location [#70](https://github.com/singer-io/tap-s3-csv/pull/70)

## 1.4.0
  * Adds proxy AWS Account support
  * [#69](https://github.com/singer-io/tap-s3-csv/pull/69)

## 1.3.9
  * Handle S3 files race condition
  * [#67](https://github.com/singer-io/tap-s3-csv/pull/67)

## 1.3.8
  * Add Missing Type Information in JSON Schema
  * [#62](https://github.com/singer-io/tap-s3-csv/pull/62)

## 1.3.7
  * Remove Backoff for Access Denied errors
  * Add unitttest for access denied giveup
  * [#55](https://github.com/singer-io/tap-s3-csv/pull/55)

## 1.3.6
  *  Implement request timeout [#46](https://github.com/singer-io/tap-s3-csv/pull/46)
  *  Updgrade SDK version [#51](https://github.com/singer-io/tap-s3-csv/pull/51)
  *  Add integration tests [#52](https://github.com/singer-io/tap-s3-csv/pull/52)

## 1.3.5
  * Increase the limit on the width of a field in the CSV files read by the tap [#47](https://github.com/singer-io/tap-s3-csv/pull/47)

## 1.3.4
  * Bump singer-encodings to 0.1.2 [#45](https://github.com/singer-io/tap-s3-csv/pull/45)

## 1.3.3
  * Bump singer-encodings to 0.1.1 [#41](https://github.com/singer-io/tap-s3-csv/pull/41)

## 1.3.2
  * Skip files without a name [#37](https://github.com/singer-io/tap-s3-csv/pull/37)
  * Fix an issue to allow the tap to run with a catalog without schemas [#38](https://github.com/singer-io/tap-s3-csv/pull/38)

## 1.3.1
  * Fixed bug that caused `integer`s to be discovered as `number` differently in different versions of python [#35](https://github.com/singer-io/tap-s3-csv/pull/35)

## 1.3.0
  * Adds support for Compressed files [#32](https://github.com/singer-io/tap-s3-csv/pull/32)
  * Adds support for JSONL files [#31](https://github.com/singer-io/tap-s3-csv/pull/31)
  * Adds support for duplicated headers in CSV files [#30](https://github.com/singer-io/tap-s3-csv/pull/30)
  * Adds testing [#29](https://github.com/singer-io/tap-s3-csv/pull/29)
  * Updates `backoff`, `singer-encodings`, and `singer-python` dependencies
  * Updates logging messages

## 1.2.3
  * Fix issue relating to search_prefix config values

## 1.2.2
  * Accepts the table key_properties as both a list and a csv string

## 1.0.4
  * Skip files when they are empty [#17](https://github.com/singer-io/tap-s3-csv/pull/17)

## 1.0.3
  * Attempt to setup a AWS session that can refresh itself [#16](https://github.com/singer-io/tap-s3-csv/pull/16)

## 1.0.2
  * Strips hyphens from incoming account_ids.

## 1.0.1
  * Adds retry logic and exponential backoff for AWS requests that fail [#10](https://github.com/singer-io/tap-s3-csv/pull/10)

## 1.0.0
  * Updates how the tap authenticates with AWS - it will now assume a role given via the config [#9](https://github.com/singer-io/tap-s3-csv/pull/9)

## 0.0.7
  * Adds support for different types of CSV delimiters via singer-encodings [#7](https://github.com/singer-io/tap-s3-csv/pull/7)

## 0.0.6
  * Add JSON Schema type string to all fields as a final fallback in case we miss a crucial sample [#5](https://github.com/singer-io/tap-s3-csv/pull/5)
  * Add error handling to fail quickly when no streams are found
  * Fixes a bug where we could sample the reskey field for a non-rectangular csv [#4](https://github.com/singer-io/tap-s3-csv/pull/4)

## 0.0.5
  * Use the singer-encodings library to share more code
  * Renames columns like "_s3_source_bucket" to "_sdc_source_bucket" to better align with standards
  * Lowers the sample rate to improve speed for larger files
  * Fixes a logging bug with a cached value

## 0.0.4
  * Header keys are no longed scrubbed for non-word characters
  * Raise an error when a date_override is set but does not exist in the header
