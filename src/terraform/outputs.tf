output "staging_data_prep_job" {
  value = aws_glue_job.staging_data_prep_job.name
}

# output "metadata_glue_job" {
#   value = aws_glue_job.metadata_etl_job.name
# }

output "glue_role" {
  value = aws_iam_role.glue_role.name
}