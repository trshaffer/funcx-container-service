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


REPO2DOCKER_CMD = 'jupyter-repo2docker --no-run --image-name funcx_{} {}'
SINGULARITY_CMD = 'singularity build {} docker-daemon://{}}'
DOCKER_BASE_URL = 'unix://var/run/docker.sock'


def docker_size(container_id):
    docker_client = docker.APIClient(base_url=DOCKER_BASE_URL)
    try:
        inspect = docker_client.inspect_image(f'funcx_{container_id}')
        return inspect['VirtualSize']
    except ImageNotFound:
        return None


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
    return await repo2docker_build(container_id, tmp_dir)


async def build_tarball(container_id, tarball, tmp_dir):
    with tarfile.open(tarball) as tar_obj:
        tar_obj.extractall(path=tmp_dir)

    # For some reason literally any file will pass through this tarfile check
    if len(os.listdir(tmp_dir)) == 0:
        raise HTTPException(status_code=415, detail="Invalid tarball")

    return await repo2docker_build(container_id, tmp_dir)


async def repo2docker_build(container_id, temp_dir):
    with tempfile.NamedTemporaryFile() as out:
        proc = await asyncio.create_subprocess_shell(
                REPO2DOCKER_CMD.format(container_id, temp_dir),
                stdout=out, stderr=out)
        await proc.communicate()
        #push log and container
    return ('docker_url', 'docker_log')


async def singularity_build(container_id):
    with tempfile.NamedTemporaryFile() as sif,
            tempfile.NamedTemporaryFile() as out:
        proc = await asyncio.create_subprocess_shell(
                SINGULARITY_CMD.format(sif, f'funcx_{container_id}'),
                stdout=out, stderr=out)
        await proc.communicate()
        #upload
        return ('singularity_url', 'singularity_log', 'singularity_size')


async def background_build(container_id, tarball):
    if not db.start_build(container_id):
        return

    with db.session_scope() as session:
        container = session.query(db.Container).filter(db.Container.id == container_id).one()
        try:
            #check/update urls
            with tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                if container.specification:
                    assert(not tarball)
                    (container.docker_url,
                    container.docker_log) = await build_spec(
                            container_id,
                            ContainerSpec.parse_raw(container.specification),
                            tmp)
                elif container.tarball:
                    (container.docker_url,
                    container.docker_log) = await build_tarball(
                            container_id,
                            tarball,
                            tmp)
            container.docker_size = docker_size(container_id)
            session.commit()

            #check/update urls
            (container.singularity_url,
            container.singularity_log,
            container.singularity_size) = await singularity_build(container_id)
        finally:
            container.building = None

    landlord.cleanup()
