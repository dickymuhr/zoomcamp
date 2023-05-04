from prefect.infrastructure.docker import DockerContainer

# To create DockerContainer block in the orion UI
docker_block = DockerContainer(
    image="dickymuhr/prefect:zoom",
    image_pull_policy="ALWAYS",
    auto_remove=True,
    network_mode="bridge"
)

docker_block.save("zoomcamp-docker", overwrite=True)