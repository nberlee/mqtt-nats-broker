# Security Policy

## Reporting a vulnerability

Please report security issues privately via GitHub's **"Report a vulnerability"**
(Security Advisories) on this repository, rather than opening a public issue.

Include a description, affected version/commit, and reproduction steps. We aim to
acknowledge within 5 business days and to coordinate a fix and disclosure
timeline with you.

## Scope

- The MQTT listener and its auth-passthrough handling.
- Handling of untrusted MQTT packet input.
- The container image and release artifacts.

NATS cluster hardening itself is out of scope — see the NATS security docs.

## Supported versions

Until a `1.0` release, only the latest tagged release receives security fixes.
