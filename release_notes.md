## Release Notes

### v3.1.2

**[New Features]**
- The process of loading proto files from disk respects logging verbosity levels.
- The offset of the consumed message can be optionally included in the output.

**[Changes]**
 - `-U` (for SASL username) and `-P` (for SASL password) short flags have been removed.
 - `Partition` and `Key` metadata will be printed to the output as separate lines for non-json formats.
 - `UTC` suffix has been removed from time strings. 

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
