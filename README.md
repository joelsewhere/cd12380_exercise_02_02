# Exercise: Extending an Existing Pipeline

Scenario
--------
This DAG defined in `inventory_pipeline.py` processes daily shipment data for a warehouse. It already runs in production and does three things:

  1. Fetches incoming shipment records
  2. Validates stock levels against thresholds
  3. Writes a stock summary to the warehouse database

You have been asked to extend it with three new additions:

  A. A new `enrich_shipments` task must be inserted between
     `validate_shipments` and `write_stock_summary`. It adds a
     warehouse region label to each shipment record. The existing
     task dependencies must be updated to route data through it.

  B. Two other teams have built DAGs that should react to this pipeline's
     outputs as soon as they are ready:
       - The finance team's DAG runs when the stock summary is written.
       - The logistics team's DAG runs when validation passes.
     Define the required Asset objects and declare them as outlets on the
     correct existing tasks.

  C. A downstream anomaly-detection DAG must be notified whenever this
     pipeline completes. Add a TriggerDagRunOperator after write_stock_summary.

Read the existing wiring carefully before making any changes.

Replace `### YOUR CODE HERE` with your code!