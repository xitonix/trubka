## Release Notes
### v3.2.1
**[Fixes]**
- Print the parsed content instead of the raw template when publishing plain text with `-gv` flags.

### v3.2.0

**[New Features]**

- `produce plain` command now supports data templates to push randomly generated messages to Kafka.
- Added `--sleep=Duration` parameter to `produce` commands so that we can put a gap between publish operations.
- `produce` commands now support `--count=0` to allow publishing to Kafka indefinitely. 

### v3.1.2

**[New Features]**
- The process of loading proto files from disk respects logging verbosity levels.
- The offset of the consumed message can be optionally included in the output.
- Different time-based starting offsets can be defined for different partitions.
- Predefined starting offsets (eg. `oldest`, `newest`, `local`, etc) can be defined for individual partitions.
- The following new flags have been added to `consume` commands: 
    - `--to`: To define a stop offset/timestamp.
    - `--idle-timeout`: The amount of time the consumer will wait for a message to arrive before stop consuming from a partition.
    - `--exclusive`: Only explicitly defined partitions (Partition#Offset|Timestamp) will be consumed. The rest will be excluded.

**[Changes]**
 - `-U` (for SASL username) and `-P` (for SASL password) short flags have been removed.
 - `Partition` and `Key` metadata will be printed to the output as separate lines for non-json formats.
 - `UTC` suffix has been replaced with timezone offsets.
 - `--from` is now a repeatable flag instead of a single comma separated string.
 - Partition-Offset delimiter has been changed to `#` for `--from` and `--to` values.
 - Wildcard offset definition syntax (`:Offset`) has been replaced with `--exclusive` flag to reduce the complexity.
 - User will not be asked to provide `topic` (or proto `schema`) in the interactive mode, if it's already been provided via command arguments. 

**[Fixes]**
- Loading proto files from disk respects termination signals received from the OS (Ctrl + C).
 
---
### v3.1.1

**[New Features]**
 - `tree` output format added to administrative commands.

**[Changes]**
- Administrative Commands
  - `list` output format has been replaced with `tree`.
  - Colours are disabled by default for Json output (`--format json`)
  - Removed clutter from `plain` output.
- Consume commands (plain/proto)
  - Timestamp (`-S, --include-timestamp`), partition key (`-K, --include-partition-key`) and topic name (`-T, --include-topic-name`) are injected into the Json output when consuming in `json` and `json-indent` modes.

**[Fixes]**

---

### v3.1.0

**[New Features]**

 - New list and json output formats for administrative commands.
 - `--style` support for json output.
 - Both `no-color` and `no-colour` flags have the same effect.
 
 **[Changes]**
 - Removed all the decorations from `--format=plain`.
 
 **[Fixes]**
 
---
 
 ### v3.0.3
 
 **[New Features]**
  - Tabular and plain text output format
 
 **[Changes]**

 
 **[Fixes]**
 
---
 
  ### v3.0.2
  
  **[New Features]**
   
  
  **[Changes]**
 
  
  **[Fixes]**
  - Random partition key generation logic for the producers.
