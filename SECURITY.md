# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in KafkaMCP, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

### How to Report

Use [**GitHub Private Vulnerability Reporting**](https://github.com/josedab/kafkamcp/security/advisories/new) to submit a security advisory. This channel is confidential and visible only to repository maintainers.

1. Navigate to **Security → Advisories → Report a vulnerability** in this repository, or use the link above.
2. Describe the vulnerability, steps to reproduce, and potential impact.
3. Provide any relevant logs, screenshots, or proof-of-concept code.

> There is no public `security@` email address for this project. GitHub private vulnerability reporting is the only supported channel.

A machine-readable [`security.txt`](https://securitytxt.org/) is provided below:

```
# security.txt (RFC 9116)
Contact: https://github.com/josedab/kafkamcp/security/advisories/new
Preferred-Languages: en
Canonical: https://github.com/josedab/kafkamcp/blob/main/SECURITY.md
Policy: https://github.com/josedab/kafkamcp/blob/main/SECURITY.md
Expires: 2027-12-31T23:59:59.000Z
```

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your report within **48 hours**.
- **Assessment**: We will investigate and provide an initial assessment within **5 business days**.
- **Resolution**: We aim to release a fix within **30 days** of confirming the vulnerability, depending on complexity.
- **Disclosure**: We will coordinate disclosure timing with you. We follow responsible disclosure practices and will credit reporters unless anonymity is requested.

### Scope

The following are in scope for security reports:

- Authentication and authorization bypass
- Injection vulnerabilities (e.g., in Kafka topic names or query parameters)
- Credential or secret exposure
- Denial of service vulnerabilities
- Insecure default configurations

### Out of Scope

- Issues in third-party dependencies (please report these upstream)
- Social engineering attacks
- Issues requiring physical access

## Security Best Practices

When deploying KafkaMCP:

- Enable TLS for all Kafka and Schema Registry connections
- Use the `default_deny: true` policy mode in production
- Configure per-agent rate limits
- Run with least-privilege Kafka ACLs
- Review audit logs regularly
