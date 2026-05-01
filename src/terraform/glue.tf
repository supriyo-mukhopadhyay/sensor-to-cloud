resource "aws_glue_job" "staging_data_prep_job" {
  name         = "${var.project}-staging-data-prep-job"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "gluejob"
    script_location = "s3://${var.glue_scripts_name}/ep011_staging_data.py"
    python_version  = 3
  }

  default_arguments = {
    "--enable-job-insights" = "true"
    "--job-language"        = "python"
    "--conf"                = "spark.rpc.message.maxSize=2000"
    "--enable-metrics"      = "true"
    "--s3_bucket"           = var.data_lake_name
    # "--source_path"         = "staging/reviews_Toys_and_Games.json.gz"
    "--target_path"         = "rnd/electronics/staging"
    "--compression"         = "snappy"
    "--partition_cols"      = jsonencode(["year", "month"])
  }

  timeout = 0

  number_of_workers = 2
  worker_type       = "G.1X"
}