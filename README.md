# (Future) EASI-FISH Analysis Pipeline

> [!WARNING]
> This pipeline is under development. 
> For the current EASI-FISH Nextflow pipeline see <https://github.com/JaneliaSciComp/multifish>

## Introduction

**JaneliaSciComp/easifish** is a bioimage analysis pipeline that reconstructs large microscopy image volumes. It ingests raw images in CZI format from Zeiss Lightsheet microscopes, computes flatfield correction and tile stitching, and outputs a multi-resolution image pyramid in N5 format. The pipeline also supports registration to a reference round as well as segmentation of selected rounds.  In the future this pipeline will support additional input formats as well as deconvolution and other image processing methods.

![EASI-FISH metro map](docs/images/JaneliaSciComp-easifish_metro_map.png)

1. Spin up a Spark cluster if configured
2. Read image metadata from MVL metadata file ([stitching-spark](https://github.com/saalfeldlab/stitching-spark/blob/master/src/main/java/org/janelia/stitching/ParseCZITilesMetadata.java))
3. Convert the CZI images to N5 format ([stitching-spark](https://github.com/saalfeldlab/stitching-spark/blob/master/src/main/java/org/janelia/stitching/ConvertCZITilesToN5Spark.java))
4. Compute flatfield correction ([stitching-spark](https://github.com/saalfeldlab/stitching-spark/blob/master/src/main/java/org/janelia/flatfield/FlatfieldCorrection.java))
5. Compute stitching ([stitching-spark](https://github.com/saalfeldlab/stitching-spark/blob/master/src/main/java/org/janelia/stitching/PipelineStitchingStepExecutor.java))
6. Fuse files and export to N5 ([stitching-spark](https://github.com/saalfeldlab/stitching-spark/blob/master/src/main/java/org/janelia/stitching/PipelineFusionStepExecutor.java))
7. Stop Spark cluster if configured
8. Register low resolution moving rounds with respect to the corresponding low resolution fixed round using [Bigstream](https://github.com/JaneliaSciComp/bigstream).
9. Spin up a [Dask cluster](https://github.com/dask) for the fine grain registration.
10. Register high resolution moving rounds with respect to the corresponding high resolution fixed round using [Bigstream](https://github.com/JaneliaSciComp/bigstream).
11. Generate the [multiscale pyramid](https://github.com/saalfeldlab/n5-spark/tree/master/src/main/java/org/janelia/saalfeldlab/n5/spark/downsample) for the aligned moving rounds
12. Generate the segmentation for the selected round(s) using [Cellpose](https://github.com/MouseLand/cellpose).

## Usage

> **Note**
> If you are new to Nextflow and nf-core, please refer to [this page](https://nf-co.re/docs/usage/installation) on how
> to set-up Nextflow. Make sure to [test your setup](https://nf-co.re/docs/usage/introduction#how-to-run-a-pipeline)
> with `-profile test` before running the workflow on actual data.

First, prepare a samplesheet with your input data that looks as follows:

`samplesheet.csv`:

```csv
id,filename,pattern
LHA3_R3_tiny,LHA3_R3_small.czi,LHA3_R3_small.czi
LHA3_R3_tiny,LHA3_R3_tiny.mvl,
LHA3_R5_tiny,LHA3_R5_small.czi,LHA3_R5_small.czi
LHA3_R5_tiny,LHA3_R5_tiny.mvl,
```

Each row represents a file in the input data set. The identifier (`id`) groups files together into acquisition rounds. In the example above, each acquisition is a single CZI file containing all of the tiles and channels, and an MVL file containing the acquisition metadata (e.g. stage coordinates for each tile.)

Now, you can run the pipeline using:

```bash
nextflow run JaneliaSciComp/easifish \
   -profile <docker/singularity/.../institute> \
   --input samplesheet.csv \
   --outdir <OUTDIR>
```

:::warning
Please provide pipeline parameters via the CLI or Nextflow `-params-file` option. Custom config files including those
provided by the `-c` Nextflow option can be used to provide any configuration _**except for parameters**_;
see [docs](https://nf-co.re/usage/configuration#custom-configuration-files).
:::

For more details and further functionality, please refer to the [usage documentation](https://nf-co.re/easifish/usage) and the [parameter documentation](https://nf-co.re/easifish/parameters).

## Pipeline output

To see the results of an example test run with a full size dataset refer to the [results](https://nf-co.re/easifish/results) tab on the nf-core website pipeline page.
For more details about the output files and reports, please refer to the
[output documentation](https://nf-co.re/easifish/output).

## Credits

The [stitching-spark tools](https://github.com/saalfeldlab/stitching-spark) used in this pipeline were developed by the [Saalfeld Lab](https://www.janelia.org/lab/saalfeld-lab) at Janelia Research Campus.

The modules for running Spark clusters on Nextflow were originally prototyped by [Cristian Goina](https://github.com/cgoina).

The JaneliaSciComp/easifish pipeline was originally constructed by [Konrad Rokicki](https://github.com/krokicki).

The workflow diagram is based on the SVG source from the [cutandrun](https://github.com/nf-core/cutandrun/) pipeline.

## Contributions and Support

If you would like to contribute to this pipeline, please see the [contributing guidelines](.github/CONTRIBUTING.md).

For further information or help, don't hesitate to get in touch on the [Slack `#easifish` channel](https://nfcore.slack.com/channels/easifish) (you can join with [this invite](https://nf-co.re/join/slack)).

## Citations

<!-- TODO nf-core: Add citation for pipeline after first release. Uncomment lines below and update Zenodo doi and badge at the top of this file. -->

If you use `JaneliaSciComp/easifish` for your analysis, please cite the EASI-FISH article as follows:

> Yuhan Wang, Mark Eddison, Greg Fleishman, Martin Weigert, Shengjin Xu, Fredrick E. Henry, Tim Wang, Andrew L. Lemire, Uwe Schmidt, Hui Yang,
> Konrad Rokicki, Cristian Goina, Karel Svoboda, Eugene W. Myers, Stephan Saalfeld, Wyatt Korff, Scott M. Sternson, Paul W. Tillberg.
> Expansion-Assisted Iterative-FISH defines lateral hypothalamus spatio-molecular organization. Cell. 2021 Dec 22;184(26):6361-6377.e24.
> doi: [10.1016/j.cell.2021.11.024](https://doi.org/10.1016/j.cell.2021.11.024). PubMed PMID: 34875226.

An extensive list of references for the tools used by the pipeline can be found in the [`CITATIONS.md`](CITATIONS.md) file.

This pipeline uses code and infrastructure developed and maintained by the [nf-core](https://nf-co.re) community, reused here under the [MIT license](https://github.com/nf-core/tools/blob/master/LICENSE).

> **The nf-core framework for community-curated bioinformatics pipelines.**
>
> Philip Ewels, Alexander Peltzer, Sven Fillinger, Harshil Patel, Johannes Alneberg, Andreas Wilm, Maxime Ulysse Garcia, Paolo Di Tommaso & Sven Nahnsen.
>
> _Nat Biotechnol._ 2020 Feb 13. doi: [10.1038/s41587-020-0439-x](https://dx.doi.org/10.1038/s41587-020-0439-x).
