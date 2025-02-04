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
  position is used to return only the actions with a _lastModifiedDate_ higher
  than the last record returned, which will ensure that no duplications are in
  place.

### Record Keys

The S3 object key uniquely identifies the objects in an Amazon S3 bucket, which
is why a record key is the key read from the S3 bucket.

## Destination

The S3 Destination Connector connects to an S3 bucket with the provided
configurations, using `aws.bucket`, `aws.accessKeyId`,`aws.secretAccessKey` and
`aws.region`. If the permissions fail, the connector will not be ready for
writing to S3.

### Writer

The S3 destination writer has a buffer with the size of `bufferSize`, for each
time `Write` is called, a new record is added to the buffer. When the buffer is
full, all the records from it will be written to the S3 bucket, and an ack
function will be called for each record after being written.<!-- /readmegen:description -->

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
        settings:
          # AWS access key id.
          # Type: string
          aws.accessKeyId: ""
          # the AWS S3 bucket name.
          # Type: string
          aws.bucket: ""
          # the AWS S3 bucket region
          # Type: string
          aws.region: ""
          # AWS secret access key.
          # Type: string
          aws.secretAccessKey: ""
          # polling period for the CDC mode, formatted as a time.Duration
          # string.
          # Type: duration
          pollingPeriod: "1s"
          # the S3 key prefix.
          # Type: string
          prefix: ""
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          sdk.schema.extract.key.enabled: "false"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          sdk.schema.extract.payload.enabled: "false"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

### Destination Configuration Parameters

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "s3"
        settings:
          # AWS access key id.
          # Type: string
          aws.accessKeyId: ""
          # the AWS S3 bucket name.
          # Type: string
          aws.bucket: ""
          # the AWS S3 bucket region
          # Type: string
          aws.region: ""
          # AWS secret access key.
          # Type: string
          aws.secretAccessKey: ""
          # the destination format, either "json" or "parquet".
          # Type: string
          format: ""
          # the S3 key prefix.
          # Type: string
          prefix: ""
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
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
