# Bazel BUILD Files

BUILD.bazel files are used by CockroachDB's build system. They must be kept in
sync with Go source files.

## When to update

Run `make gen-bazel` after any change that adds, removes, or renames `.go`
files, or changes import dependencies. This runs gazelle to regenerate
BUILD.bazel files.

## Commands

```bash
make gen-bazel    # Regenerate BUILD.bazel files
make clean-bazel  # Remove all BUILD.bazel files (for a clean regeneration)
```
