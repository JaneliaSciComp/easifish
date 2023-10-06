# JaneliaSciComp/easifish: Usage

## :warning: Please read this documentation on the nf-core website: [https://nf-co.re/easifish/usage](https://nf-co.re/easifish/usage)

> _Documentation of pipeline parameters is generated automatically from the pipeline schema and can no longer be found in markdown files._

## Introduction

**JaneliaSciComp/easifish** is a workflow for microscopy image reconstruction designed primarily for Zeiss Lightsheet microscope data.

## Samplesheet input

You will need to create a samplesheet with information about the samples you would like to analyze before running the pipeline. Use the `--input` parameter to specify its location. It should be a comma-separated file with at least 3 columns, and a header row as shown in the examples below.

`samplesheet.csv`:

```csv
id,filename,pattern
LHA3_R3_tiny,LHA3_R3_small.czi,LHA3_R3_small.czi
LHA3_R3_tiny,LHA3_R3_tiny.mvl,
LHA3_R5_tiny,LHA3_R5_small.czi,LHA3_R5_small.czi
LHA3_R5_tiny,LHA3_R5_tiny.mvl,
```

Each row represents a file in the input data set. The `filename` refers to a file in the folder provided by the `--indir` parameter.

The identifier (`id`) groups files together into acquisition rounds. In the example above there are two acquisition rounds: R3 and R5. Each acquisition round has an associated file name pattern (`pattern`) which describes all of the image files that belong to that acquisition. In the example above, each acquisition is contained in a single CZI file containing all of the tiles and channels. Each round also has an associated MVL file containing the acquisition metadata (e.g. stage coordinates for each tile.)

If you have multiple image files per acquisition, you can use the file pattern to describe them:

`samplesheet.csv`:

```csv
id,filename,pattern
LHA3_R3_tiny,LHA3_R3_tiny_V01.czi,LHA3_R3_tiny_V%02d.czi
LHA3_R3_tiny,LHA3_R3_tiny_V02.czi,
LHA3_R3_tiny,LHA3_R3_tiny.mvl,
LHA3_R5_tiny,LHA3_R5_tiny_V01.czi,LHA3_R5_tiny_V%02d.czi
LHA3_R5_tiny,LHA3_R5_tiny_V02.czi,
LHA3_R5_tiny,LHA3_R5_tiny.mvl,
```

For each tile (i.e. `<Data>`) in the MVL file, the stitcher looks for corresponding CZI file with the file name pattern. In the example above, the pattern `LHA3_R3_tiny_V%02d.czi` is used by the stitcher by replacing the variable `%02d` with incremental channel numbers, formatted with a leading zero.

### Full samplesheet

Additional samplesheet columns (`checksum` and `uri`) are supported for downloading files in cases where the data is remote. The files are downloaded to the `indir` at the start of the pipeline, and the files checksum is verified against `checksum`. If the file already exists in the `indir` folder, it is verified using the checksum and not downloaded again.

```console
id,filename,pattern,checksum,uri
LHA3_R3_tiny,LHA3_R3_tiny_V01.czi,LHA3_R3_tiny_V%02d.czi,26395350a2e33937321cd6a61b345454,https://janelia.figshare.com/ndownloader/files/30900664
LHA3_R3_tiny,LHA3_R3_tiny_V02.czi,,7373d9c07e69e33f16f166430757c408,https://janelia.figshare.com/ndownloader/files/30900676
LHA3_R3_tiny,LHA3_R3_tiny.mvl,,3b799dab4cc816b81c74652ebd7e63a3,https://janelia.figshare.com/ndownloader/files/30900679
LHA3_R5_tiny,LHA3_R5_tiny_V01.czi,LHA3_R5_tiny_V%02d.czi,9688f2cdec1c7d3d1228b3240a2a9502,https://janelia.figshare.com/ndownloader/files/30900766
LHA3_R5_tiny,LHA3_R5_tiny_V02.czi,,9a8de1ebca387dfbeb265e9fbb8ab435,https://janelia.figshare.com/ndownloader/files/30900781
LHA3_R5_tiny,LHA3_R5_tiny.mvl,,7300eaacaa089f8e6303a872a484e715,https://janelia.figshare.com/ndownloader/files/30900778
```

### Overview: Samplesheet Columns

| Column     | Description                                                                                                            |
| ---------- | ---------------------------------------------------------------------------------------------------------------------- |
| `id`       | Identifier which groups files together into acquisitions                                                               |
| `filename` | Name of file in the folder specified by the `--indir` parameter                                                        |
| `pattern`  | Filename pattern for the acquisition round. It only needs to be specified for one row associated with the acquisition. |
| `checksum` | Checksum to verify if `uri` is specified                                                                               |
| `uri`      | URI where the file can be downloaded if it is not already in the `indir`                                               |

An [example samplesheet](../assets/samplesheet.csv) has been provided with the pipeline.

## Running the pipeline

The typical command for running the pipeline is as follows:

```bash
nextflow run JaneliaSciComp/easifish --input ./assets/samplesheet.csv --outdir ./output --spark_cluster=true --spark_workers=2 --spark_worker_cores=4 --spark_gb_per_core=15
```

This will launch the pipeline with the `docker` configuration profile. See below for more information about profiles.

Note that the pipeline will create the following files in your working directory:

```bash
work                # Directory containing the nextflow working files
<OUTDIR>            # Finished results in specified location (defined with --outdir)
.nextflow_log       # Log file from Nextflow
# Other nextflow hidden files, eg. history of pipeline runs and old logs.
```

If you wish to repeatedly use the same parameters for multiple runs, rather than specifying each flag in the command, you can specify these in a params file.

Pipeline settings can be provided in a `yaml` or `json` file via `-params-file <file>`.

:::warning
Do not use `-c <file>` to specify parameters as this will result in errors. Custom config files specified with `-c` must only be used for [tuning process resource specifications](https://nf-co.re/docs/usage/configuration#tuning-workflow-resources), other infrastructural tweaks (such as output directories), or module arguments (args).
:::

The above pipeline run specified with a params file in yaml format:

```bash
nextflow run JaneliaSciComp/easifish -profile docker -params-file params.yaml
```

with `params.yaml` containing:

```yaml
input: './samplesheet.csv'
outdir: './results/'
genome: 'GRCh37'
<...>
```

You can also generate such `YAML`/`JSON` files via [nf-core/launch](https://nf-co.re/launch).

### Updating the pipeline

When you run the above command, Nextflow automatically pulls the pipeline code from GitHub and stores it as a cached version. When running the pipeline after this, it will always use the cached version if available - even if the pipeline has been updated since. To make sure that you're running the latest version of the pipeline, make sure that you regularly update the cached version of the pipeline:

```bash
nextflow pull JaneliaSciComp/easifish
```

### Reproducibility

It is a good idea to specify a pipeline version when running the pipeline on your data. This ensures that a specific version of the pipeline code and software are used when you run your pipeline. If you keep using the same tag, you'll be running the same version of the pipeline, even if there have been changes to the code since.

First, go to the [JaneliaSciComp/easifish releases page](https://github.com/JaneliaSciComp/easifish/releases) and find the latest pipeline version - numeric only (eg. `1.3.1`). Then specify this when running the pipeline with `-r` (one hyphen) - eg. `-r 1.3.1`. Of course, you can switch to another version by changing the number after the `-r` flag.

This version number will be logged in reports when you run the pipeline, so that you'll know what you used when you look back in the future. For example, at the bottom of the MultiQC reports.

To further assist in reproducbility, you can use share and re-use [parameter files](#running-the-pipeline) to repeat pipeline runs with the same settings without having to write out a command with every single parameter.

:::tip
If you wish to share such profile (such as upload as supplementary material for academic publications), make sure to NOT include cluster specific paths to files, nor institutional specific profiles.
:::

## Core Nextflow arguments

:::note
These options are part of Nextflow and use a _single_ hyphen (pipeline parameters use a double-hyphen).
:::

### `-profile`

Use this parameter to choose a configuration profile. Profiles can give configuration presets for different compute environments.

Several generic profiles are bundled with the pipeline which instruct the pipeline to use software packaged using different methods (Docker, Singularity, Podman, Shifter, Charliecloud, Apptainer, Conda) - see below.

:::info
We highly recommend the use of Docker or Singularity containers for full pipeline reproducibility, however when this is not possible, Conda is also supported.
:::

The pipeline also dynamically loads configurations from [https://github.com/nf-core/configs](https://github.com/nf-core/configs) when it runs, making multiple config profiles for various institutional clusters available at run time. For more information and to see if your system is available in these configs please see the [nf-core/configs documentation](https://github.com/nf-core/configs#documentation).

Note that multiple profiles can be loaded, for example: `-profile test,docker` - the order of arguments is important!
They are loaded in sequence, so later profiles can overwrite earlier profiles.

If `-profile` is not specified, the pipeline will run locally and expect all software to be installed and available on the `PATH`. This is _not_ recommended, since it can lead to different results on different machines dependent on the computer enviroment.

- `test`
  - A profile with a complete configuration for automated testing
  - Includes links to test data so needs no other parameters
- `docker`
  - A generic configuration profile to be used with [Docker](https://docker.com/)
- `singularity`
  - A generic configuration profile to be used with [Singularity](https://sylabs.io/docs/)
- `podman`
  - A generic configuration profile to be used with [Podman](https://podman.io/)
- `shifter`
  - A generic configuration profile to be used with [Shifter](https://nersc.gitlab.io/development/shifter/how-to-use/)
- `charliecloud`
  - A generic configuration profile to be used with [Charliecloud](https://hpc.github.io/charliecloud/)
- `apptainer`
  - A generic configuration profile to be used with [Apptainer](https://apptainer.org/)
- `conda`
  - A generic configuration profile to be used with [Conda](https://conda.io/docs/). Please only use Conda as a last resort i.e. when it's not possible to run the pipeline with Docker, Singularity, Podman, Shifter, Charliecloud, or Apptainer.

### `-resume`

Specify this when restarting a pipeline. Nextflow will use cached results from any pipeline steps where the inputs are the same, continuing from where it got to previously. For input to be considered the same, not only the names must be identical but the files' contents as well. For more info about this parameter, see [this blog post](https://www.nextflow.io/blog/2019/demystifying-nextflow-resume.html).

You can also supply a run name to resume a specific run: `-resume [run-name]`. Use the `nextflow log` command to show previous run names.

Note that running with `--spark_cluster=true` may break the `-resume`` functionality.

### `-c`

Specify the path to a specific config file (this is a core Nextflow command). See the [nf-core website documentation](https://nf-co.re/usage/configuration) for more information.

## Custom configuration

### Resource requests

This pipeline is intended for large image data and as such needs careful memory customization for each input data set. For stitching, you must specify if the Spark jobs should run locally or on a transient cluster as explained below. To change the resource requests for non-Spark jobs, please see the [max resources](https://nf-co.re/docs/usage/configuration#max-resources) and [tuning workflow resources](https://nf-co.re/docs/usage/configuration#tuning-workflow-resources) section of the nf-core website.

#### Spark Cluster

If you specify `--spark_cluster=true`, a transient Spark cluster will be created before running Spark jobs. The cluster size is controlled by the following parameters:

| Parameter              | Description                                          |
| ---------------------- | ---------------------------------------------------- |
| `--spark_workers`      | Number of distributed worker processes               |
| `--spark_worker_cores` | Number of cores per worker process                   |
| `--spark_gb_per_core`  | Amount of memory (in GB) to allocate per worker core |

The cluster's total memory size can be calculated as `spark_workers * spark_worker_cores * spark_gb_per_core` and should be adjusted based on the total data input size and hardware availability.

#### Spark Local

If you don't specify `--spark_cluster=true`, or specify `--spark_cluster=false`, then the Spark jobs will run locally. You can still control the memory limits using the same parameters, except that `--spark_workers` will be capped at 1. Since the driver process must now act as a worker, its limits are automatically adjusted. The number of cores for the driver is calculated at `spark_driver_cores + spark_worker_cores` and the amount of memory for the driver (in GB) is calculated using `2 + spark_worker_cores * spark_gb_per_core` with 2 GB of overhead. In this case, the `spark_driver_memory` parameter is ignored.

### Custom Containers

In some cases you may wish to change which container or conda environment a step of the pipeline uses for a particular tool. By default nf-core pipelines use containers and software from the [biocontainers](https://biocontainers.pro/) or [bioconda](https://bioconda.github.io/) projects. However in some cases the pipeline specified version maybe out of date.

To use a different container from the default container or conda environment specified in a pipeline, please see the [updating tool versions](https://nf-co.re/docs/usage/configuration#updating-tool-versions) section of the nf-core website.

### Custom Tool Arguments

A pipeline might not always support every possible argument or option of a particular tool used in pipeline. Fortunately, nf-core pipelines provide some freedom to users to insert additional parameters that the pipeline does not include by default.

To learn how to provide additional arguments to a particular tool of the pipeline, please see the [customizing tool arguments](https://nf-co.re/docs/usage/configuration#customising-tool-arguments) section of the nf-core website.

### nf-core/configs

In most cases, you will only need to create a custom config as a one-off but if you and others within your organization are likely to be running nf-core pipelines regularly and need to use the same settings regularly it may be a good idea to request that your custom config file is uploaded to the `nf-core/configs` git repository. Before you do this please can you test that the config file works with your pipeline of choice using the `-c` parameter. You can then create a pull request to the `nf-core/configs` repository with the addition of your config file, associated documentation file (see examples in [`nf-core/configs/docs`](https://github.com/nf-core/configs/tree/master/docs)), and amending [`nfcore_custom.config`](https://github.com/nf-core/configs/blob/master/nfcore_custom.config) to include your custom profile.

See the main [Nextflow documentation](https://www.nextflow.io/docs/latest/config.html) for more information about creating your own configuration files.

If you have any questions or issues please send us a message on [Slack](https://nf-co.re/join/slack) on the [`#configs` channel](https://nfcore.slack.com/channels/configs).

## Azure Resource Requests

To be used with the `azurebatch` profile by specifying the `-profile azurebatch`.
We recommend providing a compute `params.vm_type` of `Standard_D16_v3` VMs by default but these options can be changed if required.

Note that the choice of VM size depends on your quota and the overall workload during the analysis.
For a thorough list, please refer the [Azure Sizes for virtual machines in Azure](https://docs.microsoft.com/en-us/azure/virtual-machines/sizes).

## Running in the background

Nextflow handles job submissions and supervises the running jobs. The Nextflow process must run until the pipeline is finished.

The Nextflow `-bg` flag launches Nextflow in the background, detached from your terminal so that the workflow does not stop if you log out of your session. The logs are saved to a file.

Alternatively, you can use `screen` / `tmux` or similar tool to create a detached session which you can log back into at a later time.
Some HPC setups also allow you to run nextflow within a cluster job submitted your job scheduler (from where it submits more jobs).

## Nextflow memory requirements

In some cases, the Nextflow Java virtual machines can start to request a large amount of memory.
We recommend adding the following line to your environment to limit this (typically in `~/.bashrc` or `~./bash_profile`):

```bash
NXF_OPTS='-Xms1g -Xmx4g'
```
