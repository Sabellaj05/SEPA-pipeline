# SEPA-pipeline
data pipeline project for SEPA prices

## Run the project

```bash
# download dependencies and creates the venv
uv sync
# activate
source .venv/bin/activate
# install editable mode
uv pip install -e .
# run
uv run python -m sepa_pipeline.main
```

TODO:

- [ ] Handle more cases and implement proper testing
- [ ] Try on local linux server using cron
- [ ] Try on serverless functions like Azure Functions or AWS Lambda, store in Blob Storage or S3
- [ ] Use terraform for the cloud stuff
- [ ] Add the metadata file (pdf) and codigos provinciales (xlsx)
