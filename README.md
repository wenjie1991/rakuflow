# Rakuflow
Rakuflow is a pipeline manager, which takes advantage of the parallelism/asynchronous/concurrency feature of Raku.

Pipeline evolution: from bash scripts to Makefile, and then to more advanced pipeline tools like Cromwell, Nextflow, Snakemake, etc. The advanced tools greatly improve the reusability of code and facilitate the efficiency of batch processing.
While enjoying the convenience of the advanced tools, it is more difficult to develop and debug. (Also, complex pipelines for large samples require various techniques to achieve parallelism.)  Finally, the current tools do not allow good reuse of pipelines as modules and lacking project runtime visually monitoring features.
Rakuflow is developed in the Raku language. The power of the Raku language asynchronous made the development process enjoyable.

## New features 
Compared to existing tools:
- The progressive pipeline development. A prototype can be done with bash scripts, and then transformed into pipeline modules by one line (under developing).
- Pipeline modularity. It is possible to store (under developing) pipeline modules for easy reuse.
- Visualization of pipeline running status (under developing).
- Promise-oriented. Which is different from the Stream-oriented strategy used by Nextflow.

## Other features:
- Resume running. Like other tools, the finished jobs do not need to run again.
- Parallelism, thanks to the excellent parallelism/asynchronous/concurrency features of Raku, tasks can run parallelly based on your proposal.

## Support platforms:
- Local bash
- PBS cluster
- Slurm cluster (under development)

# How to install
TBD

# Example
```
#!/usr/bin/env raku
use lib "../rakuflow/lib";
use Rakuflow;

my $pps = Processes.new( workdir => "~/test" );
```

# TODOs
- [ ] Upload to raku module
- [ ] Support for slurm.
- [ ] Tools transform Roxygen annotated bash file into into Rakuflow.
- [ ] Command line options for re-export files when resume the tasks.
- [ ] Better display for job status.
- [ ] Pipeline modulization.
- [ ] Better history information.
- [ ] STDOUT, STDIN
