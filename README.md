## Setup
First run ```docker compose up -d``` in order to build the images. If this is your first time starting the container, it will also run init.sql and create the database and tables.

## Usage
To ingest the sp500 data, you can start the script by running ```docker compose run --rm app python app.py```. 
By default the script will run in the upsert mode and append the data to the existing.

In case you want to store only the fresh data, you can run the script in truncate mode by running ```docker compose run --rm app python app.py --truncate```.