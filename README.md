# JSON Object Loader Workflow

This process ensures that JSON objects are downloaded from a bucket, cleaned, and uploaded to the database efficiently and without redundancy.

## Workflow Steps

1. **Download JSON Objects:**
   - Run `python3 bucket_downloader.py` to download JSON objects from the bucket to the local directory.

2. **Clean JSON Objects:**
   - Run `python3 json_cleanup.py` to clean up the JSON objects. This prepares them for upload by formatting them correctly.

3. **Upload JSON Objects to the Database:**
   - Run `python3 database_uploader.py` to upload the cleaned JSON objects to the database.

## Notes

- The scripts are designed to be idempotent; they track which JSON objects have been downloaded, cleaned, and uploaded, avoiding redundant work.
- Uploaded documents are recorded in `upload_history.txt`.
- For tracking purposes, the scripts examine the contents of the `downloaded_jsons` and `cleaned_jsons` folders to determine which objects need processing.
- While we use breadcrumb messages for some operations, this method has proven inconsistent. The JSON loader described here is more reliable for our needs.

## Troubleshooting

- Ensure that there are no connectivity issues with the bucket or database.
- Verify the format of JSON objects if the cleaning step fails.
- Check `upload_history.txt` if there are uncertainties about what has been uploaded.

