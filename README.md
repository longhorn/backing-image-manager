# Longhorn Backing Image Manager [![Build Status](https://drone-publish.longhorn.io/api/badges/longhorn/backing-image-manager/status.svg)](https://drone-publish.longhorn.io/longhorn/backing-image-manager)

Longhorn Backing Image Managers handle creating, synchronizing and deleting
backing images stored on a single Longhorn disk.

For the original design, see the [Backing Image v2 LEP](https://github.com/longhorn/longhorn/blob/master/enhancements/20210701-backing-image.md).
For user information, see [Backing Image](https://longhorn.io/docs/1.6.0/advanced-resources/backing-image/backing-image/)
in the Longhorn documentation. Briefly, though, here's how this thing works:

- Backing images are either qcow2 or raw disk image files.  Backing images
  can be used when creating Longhorn volumes, so that the newly created
  volume initially contains the contents of the backing image.

- There will be one backing image manager pod running per Longhorn disk.
  These pods are created automatically by [longhorn-manager](https://github.com/longhorn/longhorn-manager).

- Backing images are stored in the `backing-images` directory of Longhorn
  disks. Within that directory there is one subdirectory named for each
  backing image (e.g. `backing-images/default-image-7b2wq-a93b9a55`), which
  in turn contains the backing image itself (a file named `backing`) and a
  config file which includes things like the backing image checksum, size
  and modification time (`backing.cfg`).

- When a backing image is first created, longhorn-manager will start a
  backing image data source pod. This pod will either accept an uploaded
  image, or will download an image from a specified URL. The image will
  initially be written to a temporary file on the Longhorn disk (e.g.
  `tmp/default-image-5wcpn-68284aaf.tmp`). Once the data transfer is
  complete, the temporary file will be moved to a subdirectory of
  `backing-images`, after which the data source pod will terminate and
  ownership of the image will be taken over by the backing image manager
  pod.

- Backing image manager pods are labelled for the disks they manage, and
  the nodes they're running on, for example:
  ```
  apiVersion: v1
  kind: Pod
  metadata:
    labels:
      longhorn.io/component: backing-image-manager
      longhorn.io/disk-uuid: 03363052-5d92-4683-837c-9b0281aaf9d6
      longhorn.io/managed-by: longhorn-manager
      longhorn.io/node: harvester-node-2
    name: backing-image-manager-03a3-0336
    namespace: longhorn-system
    [...]
  ```

- Given a backing image manager pod, it's possible to execute
  `backing-image-manager backing-image list` to get information about
   the backing images being managed, for example:
  ```
  # kubectl exec -n longhorn-system \
      backing-image-manager-03a3-0336 \
      -- backing-image-manager backing-image list
  {
    "default-image-7b2wq": {
      "name": "default-image-7b2wq",
      "uuid": "a93b9a55",
      "size": 261693952,
      "expectedChecksum": "9bef58e5c8869515d08875d5f556b363484af093c68ae67568176b429f44e19abecc970779399dd56c1be94c7b5cb954d3f05b11c8666ff09acbf77fda7b9a0f",
      "status": {
        "state": "ready",
        "currentChecksum": "9bef58e5c8869515d08875d5f556b363484af093c68ae67568176b429f44e19abecc970779399dd56c1be94c7b5cb954d3f05b11c8666ff09acbf77fda7b9a0f",
        "sendingReference": 0,
        "errorMsg": "",
        "senderManagerAddress": "",
        "progress": 100
      }
    },
    [...]
  }
  ```

- As mentioned above, backing image managers handle only a single Longhorn
  disk. Initially, when a backing image is created, it will thus only be
  present on one disk. Later, when you create a volume from that backing
  image, longhorn-manager will ask the backing image manager for each disk
  the volume is replicated on, to sync the backing image file from the
  first backing image manager.

- There's a useful SyncFile state machine diagram in
[pkg/sync/sync_file.go](https://github.com/longhorn/backing-image-manager/blob/master/pkg/sync/sync_file.go#L26-L58).
