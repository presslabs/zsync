zsync
=====

ZFS snapshot synchronization for ninjas

# Usage:

PUSH
```
zsync tank/instances@snapshot-name zfs://backup-z1.presslabs.com:tank/instances-z1

Without the @ sign == latest snapshot
```

PULL
```
zsync zfs://backup-z1.presslabs.com:tank/instances-z1@snapshot-name tank/instances
```

PUSH TO S3
```
zsync --dryrun --full tank/instances s3://bucket_name/prefix

created:
bucket_name/prefix/dataset/bulk_id
bucket_name/prefix/dataset/bulk_id/snapshot
```

PULL FROM S3
```
zsync s3://bucket_name/prefix/dataset/bulk_id@snapshot tank/instances
zsync s3://bucket_name/prefix/dataset/bulk_id tank/instances
```
