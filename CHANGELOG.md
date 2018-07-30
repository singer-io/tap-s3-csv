# Changelog

## 0.0.5
  * Use the singer-encodings library to share more code
  * Renames columns like "_s3_source_bucket" to "_sdc_source_bucket" to better align with standards
  * Lowers the sample rate to improve speed for larger files
  * Fixes a logging bug with a cached value

## 0.0.4
  * Header keys are no longed scrubbed for non-word characters
  * Raise an error when a date_override is set but does not exist in the header
