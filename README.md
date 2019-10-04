# Tesseract

[![Build Status](https://travis-ci.org/tesseract-olap/tesseract.svg?branch=master)](https://travis-ci.org/tesseract-olap/tesseract)

More documentation is coming soon!

## For Users

### Getting started

Tesseract serves an api which allows the user to drill-down, cut, filter, and otherwise examine a [cube](https://en.wikipedia.org/wiki/OLAP_cube) of data.

The logical construct of the "cube" allows for powerful, flexible, and fast data analysis while keeping the data in the most efficient physical format (in-db).

1) Get `tesseract`: See installation instructions below.
2) Get data into "cube" format: [star schema](https://en.wikipedia.org/wiki/Star_schema)-like is simplest.
3) Write a schema, which shows how the logical representation of the cube maps to the data in the database.
4) Set options as environment variables and/or cli flags. See instructions below.
5) (Optional) Set up a process monitor like systemd.
6) Run tesseract! For some examples of cli invocations, see the [justfile](https://github.com/hwchen/tesseract/blob/master/justfile). 

If you installed using Homebrew on macOS, the binary is automatically moved to your `usr/local/bin` folder and is called `tesseract-olap`.

Note: as of v0.13.0 the binary is called `tesseract-olap` on both linux and osx.

### Installation

#### macOS

Using [Homebrew](https://brew.sh/):

```
brew tap tesseract-olap/tesseract https://github.com/tesseract-olap/tesseract.git
brew install tesseract-olap
```

#### Linux

You should check the exact release on the releases page. v0.12.0 is just the first version that a deb package is available.

For now, just `wget` and `dpkg -i`. In the future, a ppa may be set up.

```
wget https://github.com/tesseract-olap/tesseract/releases/download/v0.14.2/tesseract-olap_0.14.2_amd64.deb
dpkg -i tesseract-olap_0.14.2_amd64.deb
```

You can then run the binary `tesseract-olap`.

Note that a systemd `.service` is also installed. You will probably need to modify the defaults, and you can do so at the install script prompt. To start the tesseract service, use `systemctl start tesseract-olap`.


### Environment Variables
- `TESSERACT_DATABASE_URL`: required, is the address of the database; make sure to include the user, password, and database name.
- `TESSERACT_DEBUG`: boolean, `true` is a flag to enable more verbose logging output to help the debugging process while testing.
- `TESSERACT_FLUSH_SECRET`: optional, but required for flush; is the secret key for the flush endpoint.
- `TESSERACT_LOGIC_LAYER_CONFIG_FILEPATH`: optional, should point to the location on path for the logic layer configuration.
- `TESSERACT_SCHEMA_FILEPATH`: required, should point to the location on disk for the tesseract schema file.
- `TESSERACT_STREAMING_RESPONSE`: `boolean, true` streams rows/blocks as database streaming allows.

- `RUST_LOG`: optional, sets logging level. I generally set to `info`.

### API documentation

For more details on the api, please check the server [readme](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md). This will soon be updated and easier to follow on a separate documentation site.

For more details on the logic layer api, check [here](https://github.com/hwchen/tesseract/blob/master/tesseract-server/src/logic_layer/README.md). This will also be updated and easier to follow on a separate documentation site.

## For Developers

### Dev Environment

To make life easier for developers, I've set up a simple dev environment using:
- [just](https://github.com/casey/just) (a command runner)
- [watchexec](https://github.com/watchexec/watchexec) (executes command on file changes)

You can install them via `cargo` or check their webpage:
```
cargo install just
cargo install watchexec
```

Make sure your `~/.cargo/bin` is in your `PATH`.

I also highly recommended using something like [direnv](https://github.com/direnv/direnv) to manage environment variables.

### Dev commands
From the repo root:
- `just serve`: serves from debug build, using env vars for options
- `just deploy {{target}}`: builds `--release` and will scp to target of your choice
- `just check`: an alias for `watchexec cargo check`

## Contributors

Thanks to @Datawheel for supporting this work.

Also thanks to @MarcioPorto and @jspeis for contributing.

## License

MIT license (LICENSE.md or http://opensource.org/licenses/MIT)

