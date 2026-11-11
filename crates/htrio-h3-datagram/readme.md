# H3 Datagram

This is `htrio-h3-datagram`, the optional datagram extension for `htrio-h3`,
maintained in [htrio](https://github.com/nowledge-co/htrio). It requires Rust 1.89
or later. See [UPSTREAM.md](UPSTREAM.md) for provenance. The upstream overview follows.

this crate provides an implementation of the [h3-datagram](https://datatracker.ietf.org/doc/html/rfc9297) spec that works with the h3 crate.

# Status
This crate is still in experimental. The API is subject to change. It may contain bugs and is not yet complete. Use with caution.

## Usage
As stated in the [rfc](https://datatracker.ietf.org/doc/html/rfc9297#abstract) this is intended to be used for protocol extensions like [Web-Transport](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/) and not directly by applications.

> HTTP Datagrams and the Capsule Protocol are intended for use by HTTP extensions, not applications.
