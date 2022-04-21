# Neon

Neon is a serverless open source alternative to AWS Aurora Postgres. It separates storage and compute and substitutes PostgreSQL storage layer by redistributing data across a cluster of nodes.

The project used to be called "Zenith". Many of the commands and code comments
still refer to "zenith", but we are in the process of renaming things.

## Architecture overview

A Neon installation consists of compute nodes and Neon storage engine.

Compute nodes are stateless PostgreSQL nodes, backed by Neon storage engine.

Neon storage engine consists of two major components:
- Pageserver. Scalable storage backend for compute nodes.
- WAL service. The service that receives WAL from compute node and ensures that it is stored durably.

Pageserver consists of:
- Repository - Neon storage implementation.
- WAL receiver - service that receives WAL from WAL service and stores it in the repository.
- Page service - service that communicates with compute nodes and responds with pages from the repository.
- WAL redo - service that builds pages from base images and WAL records on Page service request.

## Running local installation

1. Install build dependencies and other useful packages

On Ubuntu or Debian this set of packages should be sufficient to build the code:
```text
apt install build-essential libtool libreadline-dev zlib1g-dev flex bison libseccomp-dev \
libssl-dev clang pkg-config libpq-dev
```

[Rust] 1.58 or later is also required.

To run the `psql` client, install the `postgresql-client` package or modify `PATH` and `LD_LIBRARY_PATH` to include `tmp_install/bin` and `tmp_install/lib`, respectively.

To run the integration tests or Python scripts (not required to use the code), install
Python (3.7 or higher), and install python3 packages using `./scripts/pysync` (requires poetry) in the project directory.

2. Build neon and patched postgres
```sh
git clone --recursive https://github.com/neondatabase/neon.git
cd neon
make -j5
```

3. Start pageserver and postgres on top of it (should be called from repo root):
```sh
# Create repository in .zenith with proper paths to binaries and data
# Later that would be responsibility of a package install script
> ./target/debug/zenith init
initializing tenantid c03ba6b7ad4c5e9cf556f059ade44229
created initial timeline 5b014a9e41b4b63ce1a1febc04503636 timeline.lsn 0/169C3C8
created main branch
pageserver init succeeded

# start pageserver and safekeeper
> ./target/debug/zenith start
Starting pageserver at 'localhost:64000' in '.zenith'
Pageserver started
initializing for single for 7676
Starting safekeeper at '127.0.0.1:5454' in '.zenith/safekeepers/single'
Safekeeper started

# start postgres compute node
> ./target/debug/zenith pg start main
Starting new postgres main on timeline 5b014a9e41b4b63ce1a1febc04503636 ...
Extracting base backup to create postgres instance: path=.zenith/pgdatadirs/tenants/c03ba6b7ad4c5e9cf556f059ade44229/main port=55432
Starting postgres node at 'host=127.0.0.1 port=55432 user=zenith_admin dbname=postgres'
waiting for server to start.... done
server started

# check list of running postgres instances
> ./target/debug/zenith pg list
NODE	ADDRESS	TIMELINES	BRANCH NAME	LSN		STATUS
main	127.0.0.1:55432	5b014a9e41b4b63ce1a1febc04503636	main	0/1609610	running
```

4. Now it is possible to connect to postgres and run some queries:
```text
> psql -p55432 -h 127.0.0.1 -U zenith_admin postgres
postgres=# CREATE TABLE t(key int primary key, value text);
CREATE TABLE
postgres=# insert into t values(1,1);
INSERT 0 1
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

5. And create branches and run postgres on them:
```sh
# create branch named migration_check
> ./target/debug/zenith timeline branch --branch-name migration_check
Created timeline '0e9331cad6efbafe6a88dd73ae21a5c9' at Lsn 0/16F5830 for tenant: c03ba6b7ad4c5e9cf556f059ade44229. Ancestor timeline: 'main'

# check branches tree
> ./target/debug/zenith timeline list
 main [5b014a9e41b4b63ce1a1febc04503636]
 ┗━ @0/1609610: migration_check [0e9331cad6efbafe6a88dd73ae21a5c9]

# start postgres on that branch
> ./target/debug/zenith pg start migration_check
Starting postgres node at 'host=127.0.0.1 port=55433 user=stas'
waiting for server to start.... done

# this new postgres instance will have all the data from 'main' postgres,
# but all modifications would not affect data in original postgres
> psql -p55433 -h 127.0.0.1 -U zenith_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)

postgres=# insert into t values(2,2);
INSERT 0 1
```

6. If you want to run tests afterwards (see below), you have to stop all the running the pageserver, safekeeper and postgres instances
   you have just started. You can stop them all with one command:
```sh
> ./target/debug/zenith stop
```

## Running tests

```sh
git clone --recursive https://github.com/neondatabase/neon.git
make # builds also postgres and installs it to ./tmp_install
./scripts/pytest
```

## Documentation

Now we use README files to cover design ideas and overall architecture for each module and `rustdoc` style documentation comments. See also [/docs/](/docs/) a top-level overview of all available markdown documentation.

- [/docs/sourcetree.md](/docs/sourcetree.md) contains overview of source tree layout.

To view your `rustdoc` documentation in a browser, try running `cargo doc --no-deps --open`

### Postgres-specific terms

Due to Neon's very close relation with PostgreSQL internals, there are numerous specific terms used.
Same applies to certain spelling: i.e. we use MB to denote 1024 * 1024 bytes, while MiB would be technically more correct, it's inconsistent with what PostgreSQL code and its documentation use.

To get more familiar with this aspect, refer to:

- [Neon glossary](/docs/glossary.md)
- [PostgreSQL glossary](https://www.postgresql.org/docs/13/glossary.html)
- Other PostgreSQL documentation and sources (Neon fork sources can be found [here](https://github.com/neondatabase/postgres))

## Join the development

- Read `CONTRIBUTING.md` to learn about project code style and practices.
- To get familiar with a source tree layout, use [/docs/sourcetree.md](/docs/sourcetree.md).
- To learn more about PostgreSQL internals, check http://www.interdb.jp/pg/index.html
