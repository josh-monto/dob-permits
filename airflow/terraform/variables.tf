variable "credentials" {}
variable "project" {}

variable "region" {
  default = "us-west1"
}

variable "location" {
  default = "US"
}

variable "bq_dataset_name" {
  default = "building_permits"
}

variable "gcs_bucket_name" {}

variable "gcs_storage_class" {
  default = "STANDARD"
}

variable "parquet_file" {}