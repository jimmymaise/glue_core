  aws glue delete-job --job-name {{glue_job_name}}
  aws glue create-job \
    --name {{glue_job_name}} \
    --role {{glue_service_role}} \{% if glue_job_type == "glueetl" %}
    --glue-version "3.0"\{% endif %}
    --output json \
    --execution-property '{
      "MaxConcurrentRuns": {{max_concurrent_runs or 1}}
    }' \
    --command '{
        "Name": "{{glue_job_type}}",
        "ScriptLocation": "{{s3_deploy_url}}/glue-job.py" ,
        "PythonVersion": "3"
    }' \
    --max-capacity {{max_capacity}} \
    --default-arguments '{{default_arguments}}'
