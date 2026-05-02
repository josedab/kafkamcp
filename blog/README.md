# Blog Posts (Source)

This directory contains the **source blog posts** for KafkaMCP.

Posts here use a simple numbered prefix (e.g., `01-agents-blind-to-kafka.md`) and plain Markdown without Docusaurus frontmatter. They are the canonical versions linked from the main README.

## Relationship to `website/blog/`

The `website/blog/` directory contains **Docusaurus-formatted copies** of these posts with:

- YAML frontmatter (`slug`, `title`, `authors`, `tags`, `date`)
- Date-prefixed filenames (e.g., `2025-07-01-agents-blind-to-kafka.md`)
- Docusaurus-specific directives like `{/* truncate */}`

When updating a blog post, edit the source here first, then sync the changes to `website/blog/` with the appropriate Docusaurus frontmatter.
