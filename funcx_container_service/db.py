# not really a database, just dump everything in the fs for now

import os
import json
import uuid
import shutil
import hashlib
import tempfile
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import create_engine, func, Column, String, Integer, ForeignKey, LargeBinary, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import StaticPool

from .models import ContainerSpec, StatusResponse


Base = declarative_base()
Session = sessionmaker()
_engine = create_engine(
        'sqlite://',
        connect_args={'check_same_thread':False},
        poolclass=StaticPool)
Session.configure(bind=_engine)


class Container(Base):
    __tablename__ = 'containers'

    id = Column(String, primary_key=True)

    last_used = Column(DateTime)
    exit_status = Column(Integer)
    build_log = Column(LargeBinary)
    specification = Column(String)
    tarball = Column(LargeBinary)
    docker_size = Column(Integer)
    built = Column(Boolean, default=False)

    builds = relationship('Build', back_populates='container')


class Build(Base):
    __tablename__ = 'builds'

    id = Column(String, primary_key=True)

    container_hash = Column(String, ForeignKey('containers.id'))

    container = relationship('Container', back_populates='builds')


def total_storage():
    session = Session()
    label = 'total_storage'
    storage = session.query(Container).with_entities(func.sum(Container.docker_size).label(label)).scalar()
    return storage or 0


def hash_spec(spec):
    tmp = spec.dict()
    for k, v in tmp.items():
        if v:
            v.sort()
    canonical = json.dumps(tmp, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def store_spec(spec):
    session = Session()
    container_id = hash_spec(spec)

    for row in session.query(Container).filter(Container.id == container_id):
        return container_id, False

    cont = Container()
    cont.id = container_id
    cont.last_used = datetime.now()
    cont.specification = spec.json()
    session.add(cont)
    session.commit()
    return container_id, True


def get_spec(build_id):
    session = Session()
    for row in session.query(Build).filter(Build.id == build_id):
        build = row
        break
    else:
        raise HTTPException(status_code=404)

    spec = build.container.specification
    if not spec:
        raise HTTPException(status_code=400)

    return json.loads(spec)


def hash_tarball(tarball):
    digest = hashlib.sha256()
    with open(tarball, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()


def store_tarball(tarball):
    session = Session()

    tmp_fd, tmp_path = tempfile.mkstemp()
    os.close(tmp_fd)
    with open(tmp_path, 'wb') as f:
        shutil.copyfileobj(tarball, f)
    container_id = hash_tarball(tmp_path)

    for row in session.query(Container).filter(Container.id == container_id):
        return container_id, tmp_path, False

    #XXX upload to S3 or wherever here
    # just stash it in the database for now

    cont = Container()
    cont.id = container_id
    cont.last_used = datetime.now()
    with open(tmp_path, 'rb') as f:
        cont.tarball = f.read()
    session.add(cont)
    session.commit()
    return container_id, tmp_path, True


def fetch_tarball(container_id):
    session = Session()
    container = session.query(Container).filter(Container.id == container_id).one()
    if not container.tarball:
        return
    tmp_fd, tmp_path = tempfile.mkstemp()
    os.close(tmp_fd)
    with open(tmp_path, 'wb') as f:
        f.write(container.tarball)
    return tmp_path


def status(build_id):
    session = Session()
    for row in session.query(Build).filter(Build.id == build_id):
        build = row
        break
    else:
        raise HTTPException(status_code=404)

    out = StatusResponse(
        id=build.id,
        recipe_checksum=build.container.id,
        docker_ready=bool(build.container.docker_size),
        docker_size=build.container.docker_size,
        last_used=build.container.last_used,
        build_status = build.container.exit_status
    )

    return out


def add_build(container_id):
    session = Session()
    build = Build()
    build.id = str(uuid.uuid4())
    build.container_hash = container_id
    session.add(build)
    session.commit()

    build.container.last_used = datetime.now()
    session.add(build)
    session.commit()
    return build.id


def start_build(container_id):
    session = Session()
    container = session.query(Container).filter(Container.id == container_id).one()
    if container.built:
        return False
    container.built = True
    session.commit()
    return True

def store_build_result(container_id, exit_status, build_log, docker_size):
    session = Session()
    container = session.query(Container).filter(Container.id == container_id).one()
    container.exit_status = exit_status
    container.docker_size = docker_size
    with build_log.open('rb') as f:
        #TODO either check length or upload to S3
        container.build_log = f.read()

    session.commit()

def get_build_output(build_id):
    session = Session()
    for row in session.query(Build).filter(Build.id == build_id):
        build = row
        break
    else:
        raise HTTPException(status_code=404)
    return build.container.build_log


def docker_url(build_id):
    session = Session()
    for row in session.query(Build).filter(Build.id == build_id):
        build = row
        break
    else:
        raise HTTPException(status_code=404)
    return build.container.id, 'TODO aws' if build.container.docker_size else None


Base.metadata.create_all(_engine)
