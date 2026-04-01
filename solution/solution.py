# # Exercise: Extending an Existing Pipeline (SOLUTION)
# #
# # Scenario
# # --------
# # This DAG processes daily shipment data for a warehouse. It already runs
# # in production and does three things:
# #
# #   1. Fetches incoming shipment records
# #   2. Validates stock levels against thresholds
# #   3. Writes a stock summary to the warehouse database
# #
# # You have been asked to extend it with three new additions:
# #
# #   A. A new `enrich_shipments` task must be inserted between
# #      `validate_shipments` and `write_stock_summary`. It adds a
# #      warehouse region label to each shipment record. The existing
# #      wiring must be updated to route data through it.
# #
# #   B. Two other teams have built DAGs that should react to this pipeline's
# #      outputs as soon as they are ready:
# #        - The finance team's DAG runs when the stock summary is written.
# #        - The logistics team's DAG runs when validation passes.
# #      Define the required Asset objects and declare them as outlets on the
# #      correct existing tasks.
# #
# #   C. A downstream anomaly-detection DAG must be notified whenever this
# #      pipeline completes. Add a TriggerDagRunOperator after write_stock_summary.
# #
# # Read the existing wiring carefully before making any changes.
# # Replace `### YOUR CODE HERE` with your code!

# from datetime import datetime

# from airflow.sdk import DAG, Asset, task
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# # ---------------------------------------------------------------------------
# # EXERCISE B — Asset definitions
# #
# # Define two Asset objects that downstream DAGs will use as their schedule:
# #
# #   stock_summary_asset  — URI: "/workspace/external_storage/daily_summary.csv"
# #   validation_asset     — URI: "/workspace/external_storage/validated"
# #
# # Define them at module level so they can be referenced both as outlets
# # below and imported by the downstream DAGs that consume them.
# # ---------------------------------------------------------------------------

# stock_summary_asset = Asset("/workspace/external_storage/daily_summary.csv")
# validation_asset    = Asset("/workspace/external_storage/validated")


# # ---------------------------------------------------------------------------
# # Helpers (do not modify)
# # ---------------------------------------------------------------------------

# REGION_MAP = {
#     "north": "EMEA",
#     "south": "APAC",
#     "east":  "AMER",
#     "west":  "AMER",
# }

# def _fetch_shipments(date_str: str) -> list[dict]:
#     return [
#         {"id": "S001", "sku": "WGT-A", "qty": 120, "warehouse": "north"},
#         {"id": "S002", "sku": "GDG-B", "qty":  45, "warehouse": "south"},
#         {"id": "S003", "sku": "WGT-A", "qty":  80, "warehouse": "north"},
#         {"id": "S004", "sku": "DHK-C", "qty":  15, "warehouse": "east"},
#     ]

# def _check_thresholds(shipments: list[dict]) -> list[dict]:
#     LOW_STOCK = 50
#     return [s for s in shipments if s["qty"] >= LOW_STOCK]

# def _write_summary(enriched: list[dict], date_str: str) -> None:
#     print(f"Writing {len(enriched)} enriched shipments for {date_str} to warehouse DB")


# # ---------------------------------------------------------------------------
# # DAG definition
# # ---------------------------------------------------------------------------

# with DAG(
#     dag_id="inventory_pipeline_solution",
#     tags=["exercise", "inventory"],
# ):

#     # -----------------------------------------------------------------------
#     # Task 1 — fetch raw shipment records (do not modify)
#     # -----------------------------------------------------------------------
#     @task
#     def fetch_shipments(**context) -> list[dict]:
#         date_str = context["ds"]
#         shipments = _fetch_shipments(date_str)
#         print(f"Fetched {len(shipments)} shipments for {date_str}")
#         return shipments

#     # -----------------------------------------------------------------------
#     # Task 2 — validate stock levels (do not modify the function body)
#     #
#     # EXERCISE B (part 1) — add `outlets=[validation_asset]` to the
#     # @task decorator so the logistics team's DAG is triggered on completion.
#     # -----------------------------------------------------------------------
#     @task(outlets=[validation_asset])
#     def validate_shipments(shipments: list[dict]) -> list[dict]:
#         validated = _check_thresholds(shipments)
#         print(f"{len(validated)} of {len(shipments)} shipments passed threshold check")
#         return validated

#     # -----------------------------------------------------------------------
#     # EXERCISE A — insert enrich_shipments between validate and write
#     #
#     # Create a @task called `enrich_shipments` that:
#     #   - Receives the validated shipment list from validate_shipments
#     #   - Adds a "region" key to each record by looking up
#     #     REGION_MAP[shipment["warehouse"]]
#     #   - Returns the enriched list
#     #
#     # This task must sit between validate_shipments and write_stock_summary.
#     # Update the wiring section at the bottom so that:
#     #   validate_shipments → enrich_shipments → write_stock_summary
#     # -----------------------------------------------------------------------

#     @task
#     def enrich_shipments(validated: list[dict]) -> list[dict]:
#         for shipment in validated:
#             shipment["region"] = REGION_MAP[shipment["warehouse"]]
#         print(f"Enriched {len(validated)} shipments with region labels")
#         return validated

#     # -----------------------------------------------------------------------
#     # Task 3 — write stock summary (do not modify the function body)
#     #
#     # EXERCISE B (part 2) — add `outlets=[stock_summary_asset]` to the
#     # @task decorator so the finance team's DAG is triggered on completion.
#     # -----------------------------------------------------------------------
#     @task(outlets=[stock_summary_asset])
#     def write_stock_summary(enriched: list[dict], **context) -> None:
#         _write_summary(enriched, context["ds"])

#     # -----------------------------------------------------------------------
#     # EXERCISE C — add a TriggerDagRunOperator
#     #
#     # After write_stock_summary completes, trigger the anomaly detection DAG.
#     #
#     # Requirements:
#     #   - task_id:             "trigger_anomaly_detection"
#     #   - trigger_dag_id:      "anomaly_detection"
#     #   - wait_for_completion: False
#     #   - conf:                {"source_dag": "inventory_pipeline", "run_date": "{{ ds }}"}
#     #
#     # Wire it as the final step after write_stock_summary.
#     # -----------------------------------------------------------------------

#     trigger_anomaly_detection = TriggerDagRunOperator(
#         task_id="trigger_anomaly_detection",
#         trigger_dag_id="anomaly_detection",
#         wait_for_completion=False,
#         conf={"source_dag": "inventory_pipeline", "run_date": "{{ ds }}"},
#     )

#     # -----------------------------------------------------------------------
#     # Wiring — update this section to complete Exercises A and C.
#     #
#     # Current chain:
#     #   fetch_shipments >> validate_shipments >> write_stock_summary
#     #
#     # After Exercise A, enrich_shipments must sit between validate and write:
#     #   fetch_shipments >> validate_shipments >> enrich_shipments >> write_stock_summary
#     #
#     # After Exercise C, trigger_anomaly_detection follows write_stock_summary.
#     # -----------------------------------------------------------------------
#     shipments = fetch_shipments()
#     validated = validate_shipments(shipments)
#     enriched  = enrich_shipments(validated)
#     summary   = write_stock_summary(enriched)
#     summary >> trigger_anomaly_detection