# Commands

Commands are a way to interact with the generated repositories per cycle. They can hook into all stages of a cycle's lifetime, and influence the contents of the generated repository. For example, only a subset of a range in the configuration file may be valid for the chosen command, therefore the bounds would be modified to suit the command. This is useful for testing specific functionality.

## Merge

Merge specifically tests the `dolt merge` functionality, and therefore generates mergeable repositories and tracks how a merge operation should go between the tables.

### Merge Configurable Options

Coming Soon™
