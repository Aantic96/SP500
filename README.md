First run ```docker compose up -d --build``` in order to build the images.

Then, whenever you need to ingest the sp500 data, you need to run ```docker compose run --rm app python app.py```. 
The logic in there will upsert data based on the stock_id and the date.

In case you want to truncate tables and get store only the fresh data, you can run ```docker compose run --rm app python app.py --truncate```.