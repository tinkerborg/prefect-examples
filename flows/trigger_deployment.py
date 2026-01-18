from prefect import flow, get_client
from prefect.deployments import run_deployment
from uuid import UUID
from prefect.client.schemas.filters import FlowRunFilter, DeploymentFilter

@flow(log_prints=True)
def trigger_deployment(deployment_id: str):
    """
    Audit flow that programmatically starts a run for a given deployment ID.
    """
    print(f"Received event for new deployment: {deployment_id}")

    with get_client(sync_client=True) as client:
        flow_runs = client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                deployment_id={"any_": [deployment_id]}
            ),
            limit=100
        )
        
        print(f"Found {len(flow_runs)} runs")
        
        for run in flow_runs:
            print(f"Run: {run.id}")
            print(f"  State: {run.state.type}")
            print(f"  Created: {run.created}")
            
            # Check what triggered it
            if run.created_by:
                print(f"  Created by: {run.created_by}")
            
            # Check for automation info
            if run.state_details:
                print(f"  State details: {run.state_details}")

        deployment = client.read_deployment(deployment_id)
        
        # Only run if it has the tag
        if "initial-deployment" in deployment.tags:
            flow_run = client.create_flow_run_from_deployment(deployment_id=deployment_id)
    
            print(f"Successfully triggered run {flow_run.id} for deployment {deployment_id}")
        else: 
            print(f"Did not trigger deployment {deployment_id}")


if __name__ == "__main__":
    # Deploy this audit flow once so it has a stable UUID
    trigger_newly_created_deployment.serve(name="audit-deployment-trigger")

