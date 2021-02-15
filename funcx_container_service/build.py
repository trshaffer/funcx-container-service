import os
import json
import asyncio
import tarfile
import tempfile
import shutil
import docker
import logging
from pathlib import Path
from docker.errors import ImageNotFound
from fastapi import HTTPException
from . import db, landlord
from .models import ContainerSpec


REPO2DOCKER_CMD = "jupyter-repo2docker --no-run --image-name funcx_{} {}"
DOCKER_BASE_URL = 'unix://var/run/docker.sock'


def docker_size(container_id):
    docker_client = docker.APIClient(base_url=DOCKER_BASE_URL)
    inspect = docker_client.inspect_image(f'funcx_{container_id}')
    return inspect['VirtualSize']


def docker_ready(container_id):
    docker_client = docker.APIClient(base_url=DOCKER_BASE_URL)
    try:
        docker_client.inspect_image(f'funcx_{container_id}')
        return True
    except ImageNotFound:
        return False


def env_from_spec(spec):
    out = {
        "name": "funcx-container",
        "channels": ["conda-forge"],
        "dependencies": ["pip"]
    }
    if spec.conda:
        out["dependencies"] += list(spec.conda)
    if spec.pip:
        out["dependencies"].append({"pip": list(spec.pip)})
    return out


async def build_spec(container_id, spec, tmp_dir):
    if spec.apt:
        with (tmp_dir / 'apt.txt').open('w') as f:
            f.writelines([x + '\n' for x in spec.apt])
    with (tmp_dir / 'environment.yml').open('w') as f:
        json.dump(env_from_spec(spec), f, indent=4)
    await run_repo2docker(container_id, tmp_dir)


async def build_tarball(container_id, tmp_dir, tarball):
    with tarfile.open(tarball) as tar_obj:
        tar_obj.extractall(path=tmp_dir)

    # For some reason literally any file will pass through this tarfile check
    if len(os.listdir(tmp_dir)) == 0:
        raise HTTPException(status_code=415, detail="Invalid tarball")

    await run_repo2docker(container_id, tmp_dir)


async def run_repo2docker(container_id, temp_dir):
    with tempfile.NamedTemporaryFile() as out:
        proc = await asyncio.create_subprocess_shell(
                REPO2DOCKER_CMD.format(container_id, temp_dir),
                stdout=out, stderr=out)
        await proc.communicate()
        db.store_build_result(
                container_id,
                proc.returncode,
                Path(out.name),
                docker_size(container_id))
        landlord.check_cache()


async def background_build(container_id, tarball):
    session = db.Session()
    container = session.query(db.Container).filter(db.Container.id == container_id).one()

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        if container.specification and container.exit_status is None:
            assert(not tarball)
            await build_spec(
                    container_id,
                    ContainerSpec.parse_raw(container.specification),
                    tmp)
        elif container.tarball and container.exit_status is None:
            assert(tarball)
            await build_tarball(container_id, tmp, tarball)

    if tarball:
        os.unlink(tarball)


def remove(container_id):
    client = docker.from_env()
    try:
        client.images.remove(f'funcx_{container.id}')
    except ImageNotFound:
        logging.warning(f'docker image funcx_{container.id} removed unexpectedly')
