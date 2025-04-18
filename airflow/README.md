Dataset: Building Permits NYC Open Data
Workflow (Batch processing): Airflow
Data Lake: Google Cloud Storage
Warehouse: Google BigQuery
Transformations: dbt
Dashboard: Looker or Metabase or Data Studio
Infrastructure: Terraform

These instructions are a guide to replicate this project on your own machines/accounts, if desired. A video demonstrating the functionality was also provided if you want to evaluate the project that way (it will be easier). All instructions needed to run this project are located in this README, but if more help is needed, I will reference tutorials along the way that will provide additional information. These instructions are for a Linux-based operating system.

Navigate to the directory where you want to run this project and clone with the following command:

git clone ... 

For additional information on running Terraform with GCP, reference the following tutorial: https://developer.hashicorp.com/terraform/tutorials/gcp-get-started

To begin with Terraform and GCP, first a GCP account needs to be created if you don’t currently have one.

Install Terraform from “https://developer.hashicorp.com/terraform/install” and the Google Cloud command line interface from a bash terminal using one of the two following commands:
sudo snap install google-cloud-cli

or

sudo snap install google-cloud-sdk

Log in to GCP in your browser and create a GCP project. Then create a service account with “Storage Admin” and “BigQuery Admin” roles.

Download the credentials for this account to a private folder accessible by only you.

Within the “terraform” directory of the project, create a new file called “terraform.tfvars” with the following content:

project = “{YOUR_PROJECT_ID}”
credentials = "path/to/credentials/{CREDENTIALS_FILE_NAME}.json"
gcs_bucket_name = "{NAME_OF_BUCKET}”

Make sure to create a bucket name that you are certain is unique. Something like “{YOUR_PROJECT_ID}-bucket” should work. Terraform will create this bucket for you, so no need to create it manually in GCP.

Navigate to the terraform folder in the project directory, and run the following command:

gcloud auth application-default login

Log in following the prompts. Ensure it is the same Google account you used to create your project. Them, run terraform with the following commands:

terraform init
terraform apply

Install Docker Desktop to your machine.

Before building, you need a token to use the NYC Open Data API.

Navigate to the airflow directory. Now it’s time to build (which could take a while) and run. To do this, run the following bash commands:

docker compose build

docker compose up airflow-init

Once the init process runs without errors (should return with code 0), run the following:

docker compose up

After a minute or two, all airflow services should be up and running, and the webserver should be reachable at localhost:8081, with string “airflow” as both the user and password. I chose port 8081 as I frequently already have services running on 8080.

Login credentials are user airflow and password airflow, at localhost:8081

The DAG can be triggered from the home screen.

The first run will take a long time (~10 minutes for me). Live progress can be viewed in the logs associated with the fetch_permits task, which is what will take the most time.

After it runs, a report can be added via Looker (lookerstudio.google.com). Log in with the Google account associated with the task. Then in the Reports tab, select Blank Report. Select BigQuery as the Google Connector, then select your project name, dataset (building_permits), table (permits), and check “Use issuance_date as date range dimension). Then click Add. In the next pop-up window, select “Add to Report.” Then choose the desired layout (I chose Responsive Layout).

Now add a bar chart and select gis_nta_name (the neighborhood) as the dimension, and add a filter, selecting Include, job_type, Equal to (=), NB (for new build). Then add a “Google Maps bubble map”. In the “Fields” section, under Location, click “Add dimension”, then “Add calculated field.” Name it whatever (I named it “location”). For data type, select Geo→Latitude, Longitude. In the formula field, type “location_coordinates” (it should pop up, at which point click on it). Then click “Apply.”

When you’re done, click View at the top, and you should have this:

The shapefiles used to plot the map can be downloaded from the following source:

https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/mappluto/nyc_mappluto_25v1_shp.zip


