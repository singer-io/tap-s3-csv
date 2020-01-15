# Changelog

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
