![Master Branch](https://travis-ci.org/rahulbsw/s3s3mirror.svg?branch=master)

S3 To S3 Util
==============

A utility for mirroring/archiving content from one S3 bucket to another.

Designed to be lightning-fast and highly concurrent, with modest CPU and memory requirements.

An object will be copied if and only if at least one of the following holds true:

* The object does not exist in the destination bucket.
* The size or ETag of the object in the destination bucket are different from the size/ETag in the source bucket.

When copying, the source metadata and ACL lists are also copied to the destination object.

Note: [the 2.0-stable branch](https://github.com/cobbzilla/s3s3mirror/tree/2.0-stable) supports copying to/from local directories.

### Motivation

I started with "s3cmd sync" but found that with buckets containing many thousands of objects, it was incredibly slow
to start and consumed *massive* amounts of memory. So I designed s3s3mirror to start copying immediately with an intelligently
chosen "chunk size" and to operate in a highly-threaded, streaming fashion, so memory requirements are much lower.

Running with 100 threads, I found the gating factor to be *how fast I could list items from the source bucket* (!?!)
Which makes me wonder if there is any way to do this faster. I'm sure there must be, but this is pretty damn fast.

### AWS Credentials

* s3s3mirror will first look for credentials in your system environment. If variables named AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY are defined, then these will be used.
* Next, it checks for a ~/.s3cfg file (which you might have for using s3cmd). If present, the access key and secret key are read from there.
* IAM Roles can be used on EC2 instances by specifying the --iam flag
* If none of the above is found, it will error out and refuse to run.

### System Requirements

* Java 8 and above

### Building

    mvn package

Note that s3s3mirror now has a prebuilt jar checked in to github, so you'll only need to do this if you've been playing with the source code.
The above command requires that Maven 3 is installed.

### License

s3s3mirror is available under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

### Usage

    s3s3mirror.sh [options] <source-bucket>[/src-prefix/path/...] <destination-bucket>[/dest-prefix/path/...]

### Versions

    The 1.x branch (currently master) has been in use by the most number of people and is the most battle tested.
    
    The 2.x branch supports copying between S3 and any local filesystem. It has seen heavy use and performs well, but is not as widely used as the 1.x branch.

**In the near future, the 1.x branch will offshoot from master, and the 2.x branch will be merged into master.** There are a handful of features
on the 1.x branch that have not yet been ported to 2.x. If you can live without them, I encourage you to use the 2.x branch. If you really need them,
I encourage you to port them to the 2.x branch, if you have the ability.

### Options

     -C (--cross-account-copy)     : Copy across AWS accounts. Only Resource-based
                                     policies are supported (as specified by AWS
                                     documentation) for cross account copying.
                                     Default is false (copying within same account,
                                     preserving ACLs across copies). If this option
                                     is active, we give full access to owner of the
                                     destination bucket.
     -E (--server-side-encryption) : Enable AWS managed server-side encryption
     -A (--action)                 : Possible actions are Copy, Move or Delete objects
     -P (--profile) VAL            : Use a specific profile from your credential
                                     file (~/.aws/config)
     -S (--size-only)              : Only use object size when checking for
                                     equality and ignore etags
     -X (--delete-removed)         : Delete objects from the destination bucket if
                                     they do not exist in the source bucket
     -c (--ctime) VAL              : Only copy objects whose Last-Modified date is
                                     younger/older than this many days. For other
                                     time units, use these suffixes: y (years), M
                                     (months), d (days), w (weeks), h (hours), m
                                     (minutes), s (seconds). For use suffix -
                                     (younger), + (older)
     -d (--dest-prefix) VAL        : Destination prefix (replacing the one
                                     specified in --prefix, if any)
     -e (--endpoint) VAL           : AWS endpoint to use (or set AWS_ENDPOINT in
                                     your environment)
     -i (--iam)                    : Use IAM role from EC2 instance, can only be
                                     used in AWS
     -l (--storage-class) VAL      : Specify the S3 StorageClass (Standard |
                                     ReducedRedundancy)
     -m (--max-connections) N      : Maximum number of connections to S3 (default
                                     100)
     -n (--dry-run)                : Do not actually do anything, but show what
                                     would be done
     -p (--prefix) VAL             : Only copy objects whose keys start with this
                                     prefix
     -r (--max-retries) N          : Maximum number of retries for S3 requests
                                     (default 5)
     -s (--ssl)                    : Use SSL for all S3 api operations
     -t (--max-threads) N          : Maximum number of threads (default 100)
     -u (--upload-part-size) N     : The upload size (in bytes) of each part
                                     uploaded as part of a multipart request for
                                     files that are greater than the max allowed
                                     file size of 5368709120 bytes (5GB). Defaults
                                     to 4294967296 bytes (4GB).
     -v (--verbose)                : Verbose output
     -z (--proxy) VAL              : host:port of proxy server to use. Defaults to
                                     proxy_host and proxy_port defined in ~/.s3cfg,
                                     or no proxy if these values are not found in
                                     ~/.s3cfg

### Examples

Copy everything from a bucket named "source" to another bucket named "dest"

    s3s3mirror.sh source dest

Move everything from a bucket named "source" to another bucket named "dest"

    s3s3archive.sh source dest

Copy everything from "source" to "dest", but only copy objects created or modified within the past week

    s3s3mirror.sh -c 7 source dest
    s3s3mirror.sh -c 7d source dest
    s3s3mirror.sh -c 1w source dest
    s3s3mirror.sh -c 1w- source dest
    s3s3mirror.sh --ctime 1w source dest

Move everything from "source" to "dest", but only copy objects created or modified within the past week

    s3s3archive.sh -c 7 source dest
    s3s3archive.sh -c 7d source dest
    s3s3archive.sh -c 1w source dest
    s3s3archive.sh -c 1w- source dest
    s3s3archive.sh --ctime 1w source dest


Copy everything from "source" to "dest", but only copy objects created or modified before the past week
    
    s3s3mirror.sh -c 1w+ source dest

Move everything from "source" to "dest", but only copy objects created or modified before the past week
    
    s3s3archive.sh -c 1w+ source dest

Copy everything from "source/foo" to "dest/bar"

    s3s3mirror.sh source/foo dest/bar
    s3s3mirror.sh -p foo -d bar source dest

Move everything from "source/foo" to "dest/bar"

    s3s3archive.sh source/foo dest/bar
    s3s3archive.sh -p foo -d bar source dest


Copy everything from "source/foo" to "dest/bar" and delete anything in "dest/bar" that does not exist in "source/foo"

    s3s3mirror.sh -X source/foo dest/bar
    s3s3mirror.sh --delete-removed source/foo dest/bar
    s3s3mirror.sh -p foo -d bar -X source dest
    s3s3mirror.sh -p foo -d bar --delete-removed source dest

Move everything from "source/foo" to "dest/bar" and delete anything in "dest/bar" that does not exist in "source/foo"

    s3s3archive.sh -X source/foo dest/bar
    s3s3archive.sh --delete-removed source/foo dest/bar
    s3s3archive.sh -p foo -d bar -X source dest
    s3s3archive.sh -p foo -d bar --delete-removed source dest


Copy within a single bucket -- copy everything from "source/foo" to "source/bar"

    s3s3mirror.sh source/foo source/bar
    s3s3mirror.sh -p foo -d bar source source

Move within a single bucket -- copy everything from "source/foo" to "source/bar"

    s3s3archive.sh source/foo source/bar
    s3s3archive.sh -p foo -d bar source source


BAD IDEA: If copying within a single bucket, do *not* put the destination below the source

    s3s3mirror.sh/ source/foo source/foo/subfolder
    s3s3mirror.sh -p foo -d foo/subfolder source source
    s3s3archive.sh/ source/foo source/foo/subfolder
    s3s3archive.sh -p foo -d foo/subfolder source source
    
*This might cause recursion and raise your AWS bill unnecessarily*



###### If you've enjoyed using s3s3mirror and are looking for a warm-fuzzy feeling, consider dropping a little somethin' into my [tip jar](https://cobbzilla.org/tipjar.html)
