# Data Engineering Project

This project involves developing an end-to-end data pipeline using Google Cloud Platform (GCP) to collect, process, and visualize data from a public bus system. The pipeline integrates data from multiple sources and visualizes it for comprehensive analysis.

## Steps

### 1. Setup Google Cloud Platform (GCP)

#### Create VM in GCP
1. Go to [GCP Console](https://console.cloud.google.com/).
2. Create a new project.
3. Click "Create a VM" and enable the API.
4. Set the region to `us-west-1` (Oregon) and the Machine Type to `e2-micro`.
5. Click "Create" and wait for it to spin up.

#### Setup IAM and Storage
1. Go to [IAM & Admin](https://console.cloud.google.com/iam-admin/iam) and create a Service Account.
2. Assign the role "Storage Object Admin" to the service account.
3. Generate a JSON key for the service account and download it.
4. Attach the service account to your VM:
   - Stop the VM if running.
   - Edit the VM and attach the service account.
   - Start the VM again.

### 2. Data Collection

#### Create a Storage Bucket
1. Go to [Storage](https://console.cloud.google.com/storage/browser).
2. Create a bucket with a unique name, region `us-west1`, and storage class `Standard`.

#### Setup the VM
1. SSH into the VM.
2. Update the package list: `sudo apt update`.
3. Install Python and dependencies: `sudo apt install python3-venv python3-pip`.
4. Create a virtual environment and activate it: 
    ```bash
    python3 -m venv myenv
    source myenv/bin/activate
    ```
5. Install required Python packages:
    ```bash
    pip install requests pandas tqdm google-cloud-storage
    ```

#### Data Gathering Script
1. Create a directory for datasets: `mkdir Trimet_Doodle_Datasets`.
2. Create a Python script `project1_data_collection.py` to collect data from the Trimet API and save it to the bucket.

### 3. Automation

#### Setup Cron Jobs
1. SSH into your VM and run `whoami` to get your username.
2. Edit the crontab file: `crontab -e`.
3. Add the following lines to schedule the scripts:
    ```bash
    0 3 * * * /home/username/myenv/bin/python3 /home/username/project1_data_collection.py >> /home/username/cron_output.log 2>&1
    ```

### 4. Pub/Sub Integration

#### Create Pub/Sub Topic and Subscription
1. Go to [Cloud Pub/Sub](https://console.cloud.google.com/cloudpubsub/topic/list) and create a topic and subscription.

#### Data Publishing Script
1. Create a Python script `publish_breadcrumb_messages.py` to publish breadcrumb data to the Pub/Sub topic.

#### Data Receiving Script
1. Create a Python script `receive_breadcrumb_messages.py` to receive messages from the Pub/Sub topic and save them to the bucket.

### 5. Continuous Running

#### Setup Systemd Service
1. SSH into the VM and create a systemd service file `/etc/systemd/system/breadcrumb_receiver.service`:
    ```ini
    [Unit]
    Description=Breadcrumb Receiver Service
    After=multi-user.target

    [Service]
    Type=simple
    User=username
    ExecStart=/home/username/myenv/bin/python3 /home/username/receive_breadcrumb_messages.py
    Restart=always
    StandardOutput=append:/home/username/receive_breadcrumbs.log
    StandardError=inherit

    [Install]
    WantedBy=multi-user.target
    ```
2. Reload systemd daemon: `sudo systemctl daemon-reload`.
3. Enable and start the service:
    ```bash
    sudo systemctl enable breadcrumb_receiver.service
    sudo systemctl start breadcrumb_receiver.service
    ```

### 6. Data Integration

#### Integrate Stop Events with Breadcrumb Data
1. Access TriMet "Stop Events" data via API.
2. Build a new pipeline for the stop event data.
3. Integrate stop event data with breadcrumb data using SQL views.

### 7. Visualization

#### Visualize Data
1. Use MapboxGL or an alternative tool like Folium to visualize the integrated data.
2. Create visualizations to analyze bus speeds, routes, and other relevant metrics.

### 8. Schedule VM to Start/Stop Automatically
1. Go to [VM Instances](https://cloud.google.com/compute/docs/instances/schedule-instance-start-stop) and create a schedule to start and stop the VM automatically.

### Submission
- Provide a reference to the repository where your Python code is stored.
- Construct a table showing each day's processed data and corresponding metrics.

---

This `README.md` file provides a comprehensive guide to setting up, running, and managing the data engineering pipeline project. Let me know if there's anything else you need!

---

Running the code and how it all works.

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

