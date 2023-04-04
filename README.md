# stedi_spark_self_account

- The S3 bucket tri-lakehouse includes 3 folders customer, accelerometer and step_trainer corresponding to data from customers, accelerometer and step_trainer folder of the dataset. Primarily we have 3 tables containing the original datas: customer_landing, step_trainer_landing, and accelerometer_landing, each refers to a landing zone directory.

- Create the customer_landing and accelerometer_landing tables:
Simply create the tables from corresponding S3 directory.

- Santisize the customer_landing table's data to create customer_trusted table, join table and drop fields to create accelerometer_trusted, join table and drop the accelerometer fields to have customer_curated table:
Realising the duplication is noticeable during inspection, I add a dropduplications action right before ingesting data into the destination.

- Populate a Trusted Zone Glue Table that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research:
Join step_trainer_landing with customer_curated base on serialNumber then drop customer fields to have step_trainer_trusted:

- Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data:
Join step_trainer_trusted with accelerometer_trusted base on timestamp to create machine_learning_curated table.

The images, SQL and Python scripts are named after each query, DDL action and job.