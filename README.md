# Conduit Connector S3

<!-- readmegen:description -->
The S3 connector is one of [Conduit](https://github.com/ConduitIO/conduit)
builtin plugins. It provides both, a source and a destination S3 connectors.

## Source

The S3 Source Connector connects to a S3 bucket with the provided
configurations, using `aws.bucket`, `aws.accessKeyId`,`aws.secretAccessKey` and
`aws.region`. If the bucket doesn't exist, or the permissions fail, then an
error will occur. After that, the `Open` method is called to start the
connection from the provided position.

### Change Data Capture (CDC)

This connector implements CDC features for S3 by scanning the bucket for changes
every `pollingPeriod` and detecting any change that happened after a certain
timestamp. These changes (update, delete, create) are then inserted into a
buffer that is checked on each Read request.

* To capture "delete" and "update", the S3 bucket versioning must be enabled.
* To capture "create" actions, the bucket versioning doesn't matter.

#### Position Handling

The connector goes through two modes.

* Snapshot mode: which loops through the S3 bucket and returns the objects that
  are already in there. The _Position_ during this mode is the object key
  attached to an underscore, an "s" for snapshot, and the _maxLastModifiedDate_
  found so far. As an example: "thisIsAKey_s12345", which makes the connector
  know at what mode it is and what object it last read. The
  _maxLastModifiedDate_ will be used when changing to CDC mode, the iterator
  will capture changes that happened after that.

* CDC mode: this mode iterates through the S3 bucket every `pollingPeriod` and
  captures new actions made on the bucket. the _Position_ during this mode is
  the object key attached to an underscore, a "c" for CDC, and the object's
  _lastModifiedDate_ in seconds. As an example: "thisIsAKey_c1634049397". This
  position is used to resume from the last record returned.

  **CDC mode is listing based and best effort, bounded by `lookbackPeriod`.**
  It detects changes by listing the bucket, which cannot be lossless: an
  object that becomes visible in a listing late can be missed. Lossless S3
  change capture needs bucket event notifications, which this connector does
  not use. Every poll selects the objects modified since `lookbackPeriod`
  (default 5m) before the newest object seen so far, and skips versions it
  already delivered, so objects that become visible late, within that
  window, are picked up: S3 stamps an object with the time its upload began,
  a multipart upload with the time it was initiated, and lists in key order.
  The object store's own timestamps decide, so the clock of the machine
  running Conduit does not matter, with one exception: the newest timestamp
  is capped at this machine's clock, so one bogus timestamp from the future
  (a store clock that stepped forward, or a misbehaving endpoint) cannot hide
  the objects written after it. The cap lowers the window's start, which
  causes redelivery. If this machine's clock is behind the store's by more
  than `lookbackPeriod` the source logs a warning with the code
  `s3.source.host_clock_behind_store`: the window is stretched by the skew.

  Delivery is at-least-once. After a restart the objects modified within
  `lookbackPeriod` of the newest object are delivered again. The snapshot
  hands over to CDC from the newest object that existed when the snapshot
  started (found by one extra listing of the prefix, one request per 1000
  keys, at the start of the snapshot), minus `lookbackPeriod`, so CDC
  redelivers the objects written while the snapshot ran, however long it
  took, plus those within `lookbackPeriod` before it started. On a bucket
  that was bulk loaded recently this can be every object of the load.

  The versions remembered for deduplication are limited to about 256 MiB
  (peak memory of a poll is about 1.5 times that, candidates included). If
  the objects inside the window need more, the window is **shortened**
  instead of stopping the source: the oldest remembered entries are
  forgotten by whole seconds and the window starts just above them. The
  source logs a warning with the code `s3.source.dedupe_window_shrunk`
  naming the effective lookback and the number of forgotten entries. While
  the window is shortened, an object that becomes visible late by more than
  the effective lookback can be missed, so the tolerance for late-visible
  objects is lower than `lookbackPeriod`; lower `lookbackPeriod` to avoid
  the shrinking. The source stops, with the error
  `s3.source.dedupe_window_overflow` (after a warning), only if the versions
  sharing a single second of `LastModified` do not fit, or when entries
  stamped at or after this machine's present second (a store clock that is
  ahead, or bogus timestamps from the future) take the budget: a shortened
  window never starts later than the present second, because that would hide
  the objects written from now on, so the source stops instead (the message
  names the clock skew and says to fix the clock). A store clock that is ahead
  by less than `lookbackPeriod` is normal and does not stop the source as long
  as the entries stamped in the present and the future fit the budget. A
  future-stamped object can therefore raise the start of the window at most
  up to the present second, never past it. The shortened window is not
  remembered across restarts: after a restart the window is rebuilt from the
  position and shortened again, so a restart delivers the window's objects
  again (duplicates, never a skip). When more objects share a second than
  one poll can hand over, they are delivered over several polls, oldest
  first, and the newest tracked entries may be dropped from tracking to make
  room; those are delivered again later (duplicates). A catch-up after
  downtime is delivered in bounded batches, oldest first. A resume position
  with a timestamp in the future, which only an older release could have
  written, is clamped to this machine's clock with a warning.

  Known limits (listing cannot see these, or sees them too late):
  * an upload that takes longer than `lookbackPeriod` to complete is missed
    when newer objects were seen in the meantime (multipart uploads carry
    their initiation time);
  * replica buckets whose replication lag exceeds `lookbackPeriod`;
  * an identical re-put of an object within the same second is not seen as a
    change;
  * in an unversioned bucket an overwrite is reported as a create (there is
    no older version to compare with) and a delete is not reported;
  * in a versioned bucket intermediate versions that appear and are
    superseded within one poll are not emitted, only the latest; removing a
    delete marker (undelete) and permanently deleting the latest version are
    not emitted;
  * a GetObject that fails for another reason than the object having been
    deleted, for example an object archived to Glacier or Deep Archive,
    stops the source with that error.

  Objects deleted between being listed and being read are skipped, in the
  snapshot and in CDC. Values of `lookbackPeriod` above 1h log a warning and
  above 24h are rejected.

### Record Keys

The S3 object key uniquely identifies the objects in an Amazon S3 bucket, which
is why a record key is the key read from the S3 bucket.

## Destination

The S3 Destination Connector connects to an S3 bucket with the provided
configurations, using `aws.bucket`, `aws.accessKeyId`,`aws.secretAccessKey` and
`aws.region`. If the permissions fail, the connector will not be ready for
writing to S3.

### Writes and delivery

Every batch the SDK hands to the destination (`sdk.batch.size` records, or
fewer after `sdk.batch.delay`; one record per batch by default) is written
with one `PutObject` per object, and the records are acknowledged only after
`PutObject` succeeded. Delivery is at-least-once: a batch that is retried
after a failure or a restart is written again as a new object, so a reader
can see a record twice, never zero times. Object names are
`<UnixNano>.<ext>`, not deterministic.

### Server-side encryption

By default the destination sends no server-side encryption header, so the
bucket's default encryption applies. On AWS every bucket encrypts new
objects with SSE-S3 unless it is configured otherwise, and a bucket whose
default is SSE-KMS gets SSE-KMS objects. An S3-compatible store without a
default encryption configuration stores objects unencrypted. When
`aws.serverSideEncryption` is empty, `Open` reads the bucket's default
encryption (`GetBucketEncryption`, permission
`s3:GetEncryptionConfiguration`) and logs a warning with the code
`s3.destination.bucket_unencrypted` if there is none. It never refuses to
start; without the permission the check is skipped (debug log).

Set `aws.serverSideEncryption` to `AES256`, `aws:kms` or `aws:kms:dsse` to
request a mode explicitly, and `aws.sseKmsKeyId` to pick the KMS key for the
two KMS modes. An explicit mode overrides the bucket default.

Upgrading from v0.9.5 or earlier: those versions sent `AES256` on every
write, whatever the bucket default was, so an SSE-KMS bucket default was
silently replaced by SSE-S3 (#1047). To keep the old behavior, set
`aws.serverSideEncryption: AES256`.

### Partitioning (Kafka Connect S3 sink layout)

`partitioner` selects the directory layout under `prefix`:

| `partitioner` | Object key |
| --- | --- |
| `none` (default) | `<prefix>/<UnixNano>.<ext>` |
| `default` | `<prefix>/<collection>/partition=<kafka.partition>/<UnixNano>.<ext>` |
| `field` | `<prefix>/<collection>/<field>=<value>/.../<UnixNano>.<ext>` |
| `time`, `hourly`, `daily` | `<prefix>/<collection>/<time formatted with partitioner.pathFormat>/<UnixNano>.<ext>` |

`<collection>` is the `opencdc.collection` metadata, which `builtin:kafka`
sets to the topic, so with `prefix: topics` the directories are the ones the
Kafka Connect S3 sink writes, and tables or partition projections defined
over them keep working. `partitioner.pathFormat` takes Kafka Connect's
`path.format` syntax (numeric fields only), for example
`'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH`. `hourly` and `daily` are Kafka
Connect's `HourlyPartitioner` and `DailyPartitioner`. Partition values are
escaped the way Hive escapes path names (`/` becomes `%2F`).

A batch is split into one object per directory. If one of those writes
fails, the records before the first record of the failed object are
acknowledged and the rest are retried (or sent to the DLQ): nothing is
acknowledged that is not stored. A record without the value its
partitioner needs (no collection, no `kafka.partition`, no
`opencdc.createdAt`, a missing or null field) is never written under an
empty partition: it fails with the error code
`s3.destination.partition_value`.

`builtin:kafka` does not set `kafka.partition` yet, so `partitioner: default`
needs it set by a processor until it does.<!-- /readmegen:description -->

## Source Configuration Parameters

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "s3"
        type: source
        settings:
          # AWS access key id.
          # Type: string
          # Required: yes
          aws.accessKeyId: ""
          # the AWS S3 bucket name.
          # Type: string
          # Required: yes
          aws.bucket: ""
          # the AWS S3 bucket region
          # Type: string
          # Required: yes
          aws.region: ""
          # AWS secret access key.
          # Type: string
          # Required: yes
          aws.secretAccessKey: ""
          # The endpoint to connect to. Set this when using an S3-compatible
          # object store such as MinIO (for example http://localhost:9000).
          # Leave empty to use the default AWS endpoints. The AWS_ENDPOINT_URL
          # environment variable is also honored when this is empty. Note that
          # an http:// endpoint sends every request, including the SigV4
          # Authorization header that carries the access key ID, in the clear:
          # use https:// for anything that is not on localhost.
          # Type: string
          # Required: no
          aws.endpoint: ""
          # Use path-style addressing (http://endpoint/bucket/key) instead of
          # virtual-hosted addressing (http://bucket.endpoint/key). Set this to
          # true when using an S3-compatible store that does not support
          # virtual-hosted addressing. The default MinIO setup (started without
          # MINIO_DOMAIN) is one: it ignores the bucket in the host name and
          # takes the first path segment as the bucket instead. What comes back
          # depends on the key: with a prefix configured the remaining segments
          # are still a key, so PutObject fails with a 404 NoSuchBucket; without
          # a prefix there is no key left and MinIO answers with a 400
          # MalformedXML. Leave false for AWS S3.
          # Type: bool
          # Required: no
          aws.pathStyle: "false"
          # How far back, from the newest object seen, every CDC poll looks for
          # objects that became visible late. S3 stamps an object with the time
          # its upload began (a multipart upload with the time it was
          # initiated), not the time it completed, and lists in key order, so an
          # object can show up after newer ones were already delivered. Each
          # poll selects the objects modified since (newest LastModified seen -
          # lookbackPeriod) that were not delivered yet. Listing based CDC is
          # best effort and bounded by this value: an upload that completes
          # later than this value after newer objects were seen can still be
          # missed (see the connector description for the full list of known
          # limits). Cost: memory for the objects modified within the window
          # (limit about 256 MiB; beyond it the window is shortened with a
          # warning s3.source.dedupe_window_shrunk, which lowers the tolerance
          # for late-visible objects, so lower this value instead), and
          # redelivery (at-least-once) of the objects modified within the window
          # after a restart and when the snapshot hands over to CDC. Listing
          # cost does not depend on it. Values above 1h log a warning, above 24h
          # are rejected, and 0 disables the lookback.
          # Type: duration
          # Required: no
          lookbackPeriod: "5m"
          # polling period for the CDC mode, formatted as a time.Duration
          # string.
          # Type: duration
          # Required: no
          pollingPeriod: "1s"
          # the S3 key prefix.
          # Type: string
          # Required: no
          prefix: ""
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          # Required: no
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          # Required: no
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "false"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "false"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          # Required: no
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

## Destination Configuration Parameters

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "s3"
        type: destination
        settings:
          # AWS access key id.
          # Type: string
          # Required: yes
          aws.accessKeyId: ""
          # the AWS S3 bucket name.
          # Type: string
          # Required: yes
          aws.bucket: ""
          # the AWS S3 bucket region
          # Type: string
          # Required: yes
          aws.region: ""
          # AWS secret access key.
          # Type: string
          # Required: yes
          aws.secretAccessKey: ""
          # the destination format, either "json" or "parquet".
          # Type: string
          # Required: yes
          format: ""
          # The endpoint to connect to. Set this when using an S3-compatible
          # object store such as MinIO (for example http://localhost:9000).
          # Leave empty to use the default AWS endpoints. The AWS_ENDPOINT_URL
          # environment variable is also honored when this is empty. Note that
          # an http:// endpoint sends every request, including the SigV4
          # Authorization header that carries the access key ID, in the clear:
          # use https:// for anything that is not on localhost.
          # Type: string
          # Required: no
          aws.endpoint: ""
          # Use path-style addressing (http://endpoint/bucket/key) instead of
          # virtual-hosted addressing (http://bucket.endpoint/key). Set this to
          # true when using an S3-compatible store that does not support
          # virtual-hosted addressing. The default MinIO setup (started without
          # MINIO_DOMAIN) is one: it ignores the bucket in the host name and
          # takes the first path segment as the bucket instead. What comes back
          # depends on the key: with a prefix configured the remaining segments
          # are still a key, so PutObject fails with a 404 NoSuchBucket; without
          # a prefix there is no key left and MinIO answers with a 400
          # MalformedXML. Leave false for AWS S3.
          # Type: bool
          # Required: no
          aws.pathStyle: "false"
          # Server-side encryption to request for every object written: AES256
          # (SSE-S3), aws:kms (SSE-KMS) or aws:kms:dsse (DSSE-KMS). Leave empty
          # to send no encryption header, so the bucket's default encryption
          # applies (on AWS every bucket encrypts with SSE-S3 by default; an
          # S3-compatible store may store the object unencrypted). An explicit
          # value overrides the bucket default: setting AES256 on a bucket whose
          # default is SSE-KMS stores objects with SSE-S3 instead.
          # Type: string
          # Required: no
          aws.serverSideEncryption: ""
          # The KMS key ID, key ARN, alias name or alias ARN to encrypt objects
          # with. Only valid when aws.serverSideEncryption is aws:kms or
          # aws:kms:dsse; when empty with those modes, S3 uses the bucket's
          # default KMS key, or the AWS managed key aws/s3.
          # Type: string
          # Required: no
          aws.sseKmsKeyId: ""
          # The directory layout under the prefix. "none" writes every batch to
          # <prefix>/<UnixNano>.<ext>, as earlier versions do. The others
          # reproduce the directories of the Kafka Connect S3 sink's
          # partitioners, under <prefix>/<collection>/ (the collection is the
          # opencdc.collection metadata, the topic for records from
          # builtin:kafka): "default" adds partition=<kafka.partition metadata>,
          # "field" adds <field>=<value> per field in partitioner.fields, "time"
          # adds the record time formatted with partitioner.pathFormat, "hourly"
          # and "daily" are "time" with Kafka Connect's hourly and daily
          # patterns. A batch is split into one object per directory. A record
          # that lacks the value its partitioner needs is not written: it fails
          # with the error code s3.destination.partition_value.
          # Type: string
          # Required: no
          partitioner: "none"
          # The width of a time partition for partitioner "time", at least 1s
          # (Kafka Connect: partition.duration.ms). The record time is floored
          # to a multiple of it in partitioner.timezone.
          # Type: duration
          # Required: no
          partitioner.duration: "0s"
          # Comma-separated payload fields for partitioner "field", dots for
          # nested fields (Kafka Connect: partition.field.name). The payload
          # must be structured data or a raw JSON object.
          # Type: string
          # Required: no
          partitioner.fields: ""
          # The directory pattern for partitioner "time", in Kafka Connect's
          # path.format syntax: yyyy or YYYY, MM, dd, HH, mm, ss, and quoted
          # literal text, for example 'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH.
          # Text fields (MMM) are not supported.
          # Type: string
          # Required: no
          partitioner.pathFormat: ""
          # Where the time partitioners take the time from: "record" (the
          # opencdc.createdAt metadata, the Kafka record timestamp for
          # builtin:kafka), "recordField" (the payload field in
          # partitioner.timestampField: epoch milliseconds, an RFC 3339 string
          # or a time) or "wallclock" (the time of the write; a redelivered
          # record can land in a later directory).
          # Type: string
          # Required: no
          partitioner.timestampExtractor: "record"
          # The payload field for timestampExtractor "recordField", dots for
          # nested fields (Kafka Connect: timestamp.field).
          # Type: string
          # Required: no
          partitioner.timestampField: ""
          # The IANA time zone the time partitioners floor and format in.
          # Type: string
          # Required: no
          partitioner.timezone: "UTC"
          # the S3 key prefix.
          # Type: string
          # Required: no
          prefix: ""
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduitdata.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

### How to build it

Run `make`.

### Testing

Run `make test` to run all the tests. You must set the environment variables (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_REGION`)
before you run all the tests. If not set, the tests that use these variables will be ignored.

### Known Limitations

* If a pipeline restarts during the snapshot, then the connector will start scanning the objects from the beginning of
  the bucket, which could result in duplications.

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=191ed0af-67f7-4462-9fc0-13d1cb8e463c)
