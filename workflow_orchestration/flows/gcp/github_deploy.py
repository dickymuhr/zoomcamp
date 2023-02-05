from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from etl_web_to_gcs import etl_web_to_gcs_parent

github_block = GitHub.load("zoom-repo")

docker_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs_parent,
    name="github-flow",
    storage=github_block,
    entrypoint="workflow_orchestration/flows/gcp/etl_web_to_gcs.py:etl_web_to_gcs_parent"
)

if __name__ == "__main__":
    docker_dep.apply()