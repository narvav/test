# IDP Seed Upgrade Archetype

## Overview
This archetype governs the upgrade of IDP Java Seed parent artifact versions (`sdk-java-parent`, `sdk-java-library-parent`) to ensure security compliance, compatibility, and smooth migration across microservices.

## Core Principles
*   **Sequential Upgrades:** Follow the required upgrade path (e.g., 3.0.0 → 3.0.1)
*   **Prerequisite Verification:** Verify current Seed version before upgrading
*   **Security Compliance:** SAST scan must be 100% before release
*   **No Breaking Changes:** Address all deprecated APIs before upgrade
*   **Regression Testing:** Complete test suite validation required

## Upgrade Scope

### POM Changes
```xml
<parent>
    <groupId>com.att.idp</groupId>
    <artifactId>sdk-java-parent</artifactId>
    <version>3.0.1</version>
</parent>
```

### Key Dependencies (Seed 3.0.1)
| Component | Version |
|-----------|---------|
| Spring Boot | 3.5.6 |
| Spring Kafka | 3.3.10 |
| Jackson | 2.19.2 |
| JUnit BOM | 5.12.2 |
| Azure Spring | 6.1.0 |

## Available Workflows
The following workflows are available in the `.windsurf/workflows/` directory:

*   **scaffold-idp-seed-upgrade**: Generate migration plan and upgrade checklist
*   **refactor-idp-seed-upgrade**: Apply code changes for new Seed version
*   **test-idp-seed-upgrade**: Validate upgrade with regression tests
*   **compare-idp-seed-upgrade**: Compare Seed versions and breaking changes
*   **document-idp-seed-upgrade**: Generate upgrade documentation and reports
*   **debug-idp-seed-upgrade**: Troubleshoot upgrade issues and conflicts

## Usage

### Quick Start
1. Use `scaffold-idp-seed-upgrade` to generate migration plan
2. Create feature branch following naming convention
3. Use `refactor-idp-seed-upgrade` to apply code changes
4. Use `test-idp-seed-upgrade` to validate changes
5. Use `document-idp-seed-upgrade` to generate migration report

### Example
```bash
# Generate migration plan for Seed 3.0.0 → 3.0.1
/scaffold-idp-seed-upgrade --from 3.0.0 --to 3.0.1 --project usermanagementms
```

## References
- [Seed 3.0.1 Release Notes](https://wiki.web.att.com/pages/viewpage.action?spaceKey=IDSEPA&title=Seed+3.0.1+-+Release+Notes)
- [Seed 3.0.1 Upgrade Instructions](https://wiki.web.att.com/display/IDSEPA/Seed+3.0.1+-+Upgrade+Instructions)
- [FAQ 3.0.1](https://wiki.web.att.com/display/IDSEPA/FAQ+-+3.0.1)
- [IDP Java Seed Managed Dependencies](https://wiki.web.att.com/display/IDSEPA/IDP+Java+Seed+managed+dependency+versions)
