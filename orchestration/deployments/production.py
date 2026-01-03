from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from orchestration.flows.daily_metrics import daily_metrics_pipeline
from orchestration.flows.data_ingestion import data_ingestion_pipeline
from orchestration.flows.experiment_analysis import experiment_analysis_pipeline

daily_metrics_deployment = Deployment.build_from_flow(
    flow=daily_metrics_pipeline,
    name="daily-metrics-scheduled",
    schedule=CronSchedule(cron="0 6 * * *", timezone="UTC"),
    work_pool_name="default-pool",
    tags=["production", "scheduled"],
)

data_ingestion_deployment = Deployment.build_from_flow(
    flow=data_ingestion_pipeline,
    name="data-ingestion-on-demand",
    work_pool_name="default-pool",
    tags=["production", "on-demand"],
)

experiment_analysis_deployment = Deployment.build_from_flow(
    flow=experiment_analysis_pipeline,
    name="experiment-analysis-on-demand",
    work_pool_name="default-pool",
    tags=["production", "experiments"],
)


if __name__ == "__main__":
    daily_metrics_deployment.apply()
    data_ingestion_deployment.apply()
    experiment_analysis_deployment.apply()
    print("Deployments created successfully")
