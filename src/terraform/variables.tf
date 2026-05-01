variable "project" {
  type        = string
  description = "Project name"
}

variable "region" {
  type        = string
  description = "AWS region"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID"
}

variable "public_subnet_a_id" {
  type        = string
  description = "Public subnet A ID"
}

variable "data_lake_name" {
  type        = string
  description = "Data lake bucket name"
}

variable "glue_scripts_name" {
  type        = string
  description = "Glue scripts bucket name"
}
