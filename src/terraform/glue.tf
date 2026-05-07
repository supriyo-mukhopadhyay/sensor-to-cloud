resource "aws_glue_job" "staging_data_prep_job" {
  name         = "${var.project}-staging-data-prep-job"
  role_arn     = aws_iam_role.glue_role.arn
  

  command {
    name            = "gluestreaming"
    script_location = "s3://${var.glue_scripts_name}/ep011_staging_data.py"
    python_version  = "3"
  }

  default_arguments = {
    "--additional-python-modules"         = "paho.mqtt, dotenv"
  }

  execution_property {
    max_concurrent_runs = 1
  }
  
  tags = {
    "ManagedBy" = "AWS"
  }
}