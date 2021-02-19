import docker
from sqlalchemy import and_, func
from . import db, build
from .models import ContainerSpec


MAX_STORAGE = 2000000000
ALPHA = 0.5


def jaccard(a, b):
    return 1 - float(len(a & b))/len(a | b)


def spec_to_set(spec):
    out = set()
    if spec.apt:
        out.update({f'a{x}' for x in spec.apt})
    if spec.conda:
        out.update({f'c{x}' for x in spec.conda})
    if spec.pip:
        out.update({f'p{x}' for x in spec.pip})
    return out


def total_storage():
    with db.session_scope() as session:
        size = session.query(db.Container).with_entities(func.sum(db.Container.docker_size + db.Container.singularity_size)).scalar()
        return size or 0

async def cleanup():
    session = db.Session()

    while total_storage() > MAX_STORAGE:
        container = session.query(db.Container).filter(db.Container.docker_size.isnot(None)).order_by(db.Container.last_used.asc()).first()
        build.remove(container.id)
        session.commit()

def find_existing(spec):
    session = db.Session()

    target = spec_to_set(spec)
    best_id = None
    best_distance = 2.0 # greater than any jaccard distance, effectively inf.

    for container in session.query(db.Container).filter(and_(
            db.Container.docker_size.isnot(None),
            db.Container.specification.isnot(None))):
        other = spec_to_set(ContainerSpec.parse_raw(container.specification))
        if not target.issubset(other):
            continue
        distance = jaccard(target, other)
        if distance > ALPHA:
            continue
        if distance < best_distance:
            best_distance = distance
            best_id = container.id

    return best_id
