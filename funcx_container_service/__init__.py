from uuid import UUID
from typing import Optional
from fastapi import FastAPI, UploadFile, File, Response, BackgroundTasks
from . import db, build, landlord
from .models import ContainerSpec, StatusResponse
from .dockerfile import emit_dockerfile

app = FastAPI()


@app.post("/build", response_model=UUID)
def simple_build(spec: ContainerSpec, tasks: BackgroundTasks):
    """Build a container based on a JSON specification.

    Returns an ID that can be used to query container status.
    """
    container_id = db.store_spec(spec)

    alt = landlord.find_existing(spec)
    if alt:
        return db.add_build(alt)
    else:
        tasks.add_task(build.background_build, container_id, None)

    return db.add_build(container_id)


@app.post("/build_advanced", response_model=UUID)
def advanced_build(tasks: BackgroundTasks, repo: UploadFile = File(...)):
    """Build a container using repo2docker.

    The repo must be a directory in `.tar.gz` format.
    Returns an ID that can be used to query container status.
    """
    container_id = db.store_tarball(repo.file)

    tasks.add_task(build.background_build, container_id, repo.file)
    return db.add_build(container_id)


@app.get("/{build_id}/dockerfile")
def dockerfile(build_id: UUID):
    """Generate a Dockerfile to build the given container.

    Does not support "advanced build" (tarball) containers.
    Produces a container that is roughly compatible with repo2docker.
    """
    pkgs = db.get_spec(str(build_id))
    return Response(content=emit_dockerfile(pkgs['apt'], pkgs['conda'], pkgs['pip']),
                    media_type="text/plain")


@app.get("/{build_id}/status", response_model=StatusResponse)
def status(build_id: UUID):
    """Check the status of a previously submitted build.

    A "build_status" of null indicates that the build is in progress.
    On success, this status will be 0. On error, it may be helpful to
    examine /{build_id}/build_log
    """
    return db.status(str(build_id))


@app.get("/{build_id}/docker", response_model=Optional[str])
def get_docker(build_id: UUID, tasks: BackgroundTasks):
    """Get the Docker build for a container.

    If the container is not ready, null is returned, and a build is
    initiated (if not already in progress). If the build specification
    was invalid and cannot be completed, returns HTTP 410: Gone.
    """

    container_id, url = db.docker_url(str(build_id))
    if not url:
        tasks.add_task(build.background_build, container_id, None)
    return url


@app.get("/{build_id}/singularity", response_model=Optional[str])
def get_docker(build_id: UUID, tasks: BackgroundTasks):
    """Get the Docker build for a container.

    If the container is not ready, null is returned, and a build is
    initiated (if not already in progress). If the build specification
    was invalid and cannot be completed, returns HTTP 410: Gone.
    """

    container_id, url = db.singularity_url(str(build_id))
    if not url:
        tasks.add_task(build.background_build, container_id, None)
    return url
