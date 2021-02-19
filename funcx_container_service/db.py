# not really a database, just dump everything in the fs for now

import os
import json
import uuid
import shutil
import hashlib
import tempfile
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

from fastapi import HTTPException
from sqlalchemy import create_engine, func, Column, String, Integer, ForeignKey, LargeBinary, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import StaticPool

from .models import ContainerSpec, StatusResponse


RUN_ID = str(uuid.uuid4())

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
    specification = Column(String)
    tarball = Column(String)
    docker_url = Column(String)
    docker_log = Column(String)
    docker_size = Column(Integer)
    singularity_url = Column(String)
    singularity_log = Column(String)
    singularity_size = Column(Integer)
    building = Column(String)

    builds = relationship('Build', back_populates='container')


class Build(Base):
    __tablename__ = 'builds'

    id = Column(String, primary_key=True)
    container_hash = Column(String, ForeignKey('containers.id'))
    #user

    container = relationship('Container', back_populates='builds')


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def hash_file(pth):
    digest = hashlib.sha256()
    with open(pth, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()


def store_spec(spec):
    container_id = spec.digest()

    with session_scope() as session:
        for row in session.query(Container).filter(Container.id == container_id):
            return container_id

        cont = Container()
        cont.id = container_id
        cont.last_used = datetime.now()
        cont.specification = spec.json()
        session.add(cont)

    return container_id


def store_tarball(tarball):
    tarball.rollover()
    container_id = hash_file(tarball.name) #SLOW

    with session_scope() as session:
        for row in session.query(Container).filter(Container.id == container_id):
            return container_id

        cont = Container()
        session.add(cont)
        cont.id = container_id
        cont.last_used = datetime.now()

        #XXX upload to S3 or wherever here

    return container_id


def get_spec(build_id):
    with session_scope() as session:
        for row in session.query(Build).filter(Build.id == build_id):
            build = row
            break
        else:
            raise HTTPException(status_code=404)

        spec = build.container.specification
        if not spec:
            raise HTTPException(status_code=400)

        return json.loads(spec)


def add_build(container_id):
    with session_scope() as session:
        build = Build()
        build.id = str(uuid.uuid4())
        build.container_hash = container_id
        session.add(build)
        session.commit()

        build.container.last_used = datetime.now()
        session.add(build)
        return build.id


def status(build_id):
    with session_scope() as session:
        for row in session.query(Build).filter(Build.id == build_id):
            build = row
            container = row.container
            break
        else:
            raise HTTPException(status_code=404)

        #check/update urls

        return StatusResponse(
            id=build.id,
            recipe_checksum=container.id,
            last_used=container.last_used,
            docker_url=container.docker_url,
            docker_size=container.docker_size,
            docker_log=container.docker_log,
            singularity_url=container.singularity_url,
            singularity_size=container.singularity_size,
            singularity_log=container.singularity_log
            )


def docker_url(build_id):
    with session_scope() as session:
        for row in session.query(Build).filter(Build.id == build_id):
            container = row.container
            break
        else:
            raise HTTPException(status_code=404)
        #check/update urls
        if container.docker_log and not container.docker_url:
            raise HTTPException(status_code=410)
        return container.id, container.docker_url


def singularity_url(build_id):
    with session_scope() as session:
        for row in session.query(Build).filter(Build.id == build_id):
            container = row.container
            break
        else:
            raise HTTPException(status_code=404)
        #check/update urls
        if container.singularity_log and not container.singularity_url:
            raise HTTPException(status_code=410)
        return container.id, container.singularity_url


def start_build(container_id):
    with session_scope() as session:
        container = session.query(Container).filter(Container.id == container_id).populate_existing().with_for_update().one()
        if container.building == RUN_ID:
            return False
        elif container.building is not None:
            #clean up from crash
            pass
        container.building = RUN_ID
    return True


Base.metadata.create_all(_engine)
