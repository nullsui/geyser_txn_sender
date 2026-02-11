# Security Policy

## Supported Versions

Security fixes are applied to the latest `main` branch.

## Reporting a Vulnerability

Please do not open public issues for security vulnerabilities.

Use GitHub Security Advisories (private report) for this repository. Include:

- Impact and attack scenario
- Reproduction steps or proof of concept
- Suggested remediation if available

We will acknowledge reports as quickly as possible and coordinate a fix and disclosure timeline.

## Secrets Hygiene

- Never commit credentials, private keys, or production infrastructure details.
- Keep deploy credentials in `deploy/.env` only (gitignored).
- Rotate any credential immediately if it is exposed.
