import enum
import pendulum
from typing import Dict, List, NamedTuple, Any
from ..context import Context
import pykube.objects
from .abstract import Snapshot, SnapshotStatus
from ..errors import SnapshotCreateError
import digitalocean
import os
import re


@enum.unique
class SnapshotStatus(enum.Enum):
    PENDING = 'snapshot.pending'
    COMPLETE = 'snapshot.complete'


DiskIdentifier = Any

DIGITAL_OCEAN_API_TOKEN = os.environ['DIGITAL_OCEAN_API_TOKEN']


class DigitalOceanDiskIdentifier(NamedTuple):
    volume_id: str


NewSnapshotIdentifier = Any


def validate_disk_identifier(disk_id: Dict) -> DiskIdentifier:
    try:
        return DigitalOceanDiskIdentifier(
            volume_id=disk_id.id
        )
    except:
        raise ValueError(disk_id)


def load_snapshots(ctx: Context, label_filters: Dict[str, str]) -> List[Snapshot]:
    """
    Using regex to identify snapshots because filtering is not possible.
    """
    regex = r"^.*\-\d{6}\-\d{6}$"  # get all k8s snapshot created snapshots

    manager = digitalocean.Manager(token=DIGITAL_OCEAN_API_TOKEN)

    snapshots = []
    for snapshot in manager.get_all_snapshots():
        if re.search(regex, snapshot.name):
            snapshots.append(Snapshot(
                name=snapshot.id,
                created_at=parse_timestamp(snapshot.created_at),
                disk=DigitalOceanDiskIdentifier(volume_id=snapshot.resource_id)
            ))

    return snapshots


def create_snapshot(
        ctx: Context,
        disk: DiskIdentifier,
        snapshot_name: str,
        snapshot_description: str
) -> NewSnapshotIdentifier:
    """
    Creates a new snapshot of the given disk
    """
    manager = digitalocean.Manager(token=DIGITAL_OCEAN_API_TOKEN)
    volume = manager.get_volume(disk.volume_id)
    snapshot = volume.snapshot(snapshot_name)
    return snapshot


def get_snapshot_status(
        ctx: Context,
        snapshot_identifier: NewSnapshotIdentifier
) -> SnapshotStatus:
    if 'snapshot' in snapshot_identifier:
        return SnapshotStatus.COMPLETE
    else:
        raise SnapshotCreateError('failed to create a snapshot')


def set_snapshot_labels(
        ctx: Context,
        snapshot_identifier: NewSnapshotIdentifier,
        labels: Dict
):
    """
    API does not provide labels or tags for snapshots.
    """
    pass


def delete_snapshot(
        ctx: Context,
        snapshot: Snapshot
):
    manager = digitalocean.Manager(token=DIGITAL_OCEAN_API_TOKEN)
    manager.get_snapshot(snapshot.name).destroy()


def supports_volume(volume: pykube.objects.PersistentVolume):
    return volume.obj['spec'].get('storageClassName') == 'do-block-storage'


def get_disk_identifier(volume: pykube.objects.PersistentVolume):
    return DigitalOceanDiskIdentifier(volume_id=volume.obj.get('spec')['csi']['volumeHandle'])


def parse_timestamp(date_str: str) -> pendulum.Pendulum:
    return pendulum.parse(date_str).in_timezone('utc')
