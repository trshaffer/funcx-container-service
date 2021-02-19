import os
import json
import asyncio
import tarfile
import tempfile
import shutil
import docker
import logging
import boto3
from pathlib import Path
from docker.errors import ImageNotFound
from fastapi import HTTPException
from . import db, landlord
from .models import ContainerSpec


REPO2DOCKER_CMD = 'jupyter-repo2docker --no-run --image-name funcx_{} {}'
SINGULARITY_CMD = 'echo singularity build --force {} docker-daemon://funcx_{}:latest'
DOCKER_BASE_URL = 'unix://var/run/docker.sock'


async def s3_upload(s3, filename, bucket, key):
    s3.upload_file(filename, bucket, key)
    return s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key})


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


async def build_spec(s3, container_id, spec, tmp_dir):
    if spec.apt:
        with (tmp_dir / 'apt.txt').open('w') as f:
            f.writelines([x + '\n' for x in spec.apt])
    with (tmp_dir / 'environment.yml').open('w') as f:
        json.dump(env_from_spec(spec), f, indent=4)
    return await repo2docker_build(s3, container_id, tmp_dir)


async def build_tarball(s3, container_id, tarball, tmp_dir):
    with tarfile.open(tarball) as tar_obj:
        tar_obj.extractall(path=tmp_dir)

    # For some reason literally any file will pass through this tarfile check
    if len(os.listdir(tmp_dir)) == 0:
        raise HTTPException(status_code=415, detail="Invalid tarball")

    return await repo2docker_build(s3, container_id, tmp_dir)


async def repo2docker_build(s3, container_id, temp_dir):
    with tempfile.NamedTemporaryFile() as out:
        proc = await asyncio.create_subprocess_shell(
                REPO2DOCKER_CMD.format(container_id, temp_dir),
                stdout=out, stderr=out)
        await proc.communicate()

        out.flush()
        out.seek(0)
        log_url = await s3_upload(s3, out.name, 'docker-logs', container_id)

    container_size = docker_size(container_id)
    container_url = None
    if container_size:
        container_url = None # push to ECR
    return (container_url, log_url, container_size)


async def singularity_build(s3, container_id):
    with tempfile.NamedTemporaryFile() as sif, \
            tempfile.NamedTemporaryFile() as out:
        proc = await asyncio.create_subprocess_shell(
                SINGULARITY_CMD.format(sif, container_id),
                stdout=out, stderr=out)
        await proc.communicate()

        container_size = os.stat(sif.name).st_size
        container_log = await s3_upload(s3, out.name, 'singularity-logs', container_id)
        container_url = None
        if container_size > 0:
            container_url = await s3_upload(s3, sif.name, 'singularity', container_id)
        return (container_url, container_log, container_size)


async def background_build(container_id, tarball):
    if not db.start_build(container_id):
        return

    s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:9000', aws_access_key_id = 'minioadmin', aws_secret_access_key = 'minioadmin')

    with db.session_scope() as session:
        container = session.query(db.Container).filter(db.Container.id == container_id).one()
        try:
            #check/update urls
            with tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                if container.specification:
                    assert(not tarball)
                    (container.docker_url,
                    container.docker_log,
                    container.docker_size) = await build_spec(
                            s3,
                            container_id,
                            ContainerSpec.parse_raw(container.specification),
                            tmp)
                elif container.tarball:
                    (container.docker_url,
                    container.docker_log,
                    container.docker_size) = await build_tarball(
                            s3,
                            container_id,
                            tarball,
                            tmp)

            #check/update urls
            (container.singularity_url,
            container.singularity_log,
            container.singularity_size) = await singularity_build(s3, container_id)
        finally:
            container.building = None
            session.commit()

    await landlord.cleanup()

def remove(container_id):
    s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:9000', aws_access_key_id = 'minioadmin', aws_secret_access_key = 'minioadmin')
    s3.delete_object({'Bucket': 'singularity', 'Key': container_id})
    s3.delete_object({'Bucket': 'singularity-logs', 'Key': container_id})
    s3.delete_object({'Bucket': 'docker-logs', 'Key': container_id})
    #delete from ecr

    with db.session_scope() as session:
        container = session.query(db.Container).filter(db.Container.id == container_id).one()
        container.docker_url = None
        container.docker_log = None
        container.docker_size = None
        container.singularity_url = None
        container.singularity_log = None
        container.singularity_size = None
