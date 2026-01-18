from prefect import flow
from prefect.deployments import run_deployment
from uuid import UUID

@flow(log_prints=True)
def trigger_deployment(deployment_id: str):
    """
    Audit flow that programmatically starts a run for a given deployment ID.
    """
    print(f"Received event for new deployment: {deployment_id}")
    
    # Trigger the deployment. run_deployment accepts UUID or 'flow/deployment' name.
    # We use timeout=0 to return immediately so the audit flow finishes quickly.
    flow_run = run_deployment(
        deployment_id,
        timeout=0
    )
    
    print(f"Successfully triggered run {flow_run.id} for deployment {deployment_id}")

if __name__ == "__main__":
    # Deploy this audit flow once so it has a stable UUID
    trigger_newly_created_deployment.serve(name="audit-deployment-trigger")

