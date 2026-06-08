# JaneliaSciComp/easifish: Changelog

* Added support for zarr v3

* Renamed the skip_<step> parameters with run_<step> - it is less confusing

* Made the pipeline config files compliant with V2 parser

* Use templates for dask modules so that spark and dask workflows could be executed with conda not only with a container runtime.

## 69f79cd - 04/24/2026
* Configurable BigStitcher steps through a YAML file

* Option to separate the segmentation label merge for distributed cellpose

* Support spot extraction from warped image without the need to warp the spots

* Added spots warping using Bigstream

* Added FISHSPOT module for spot extraction

* Added RS-FISH module for spot extraction

* Added spots count and spots intensities

* Support two methods to merge labels from neighboring regions from adjacent blocks that were segmented indenpendently using a distributed segmentation.

* Run volume segmentation using distributed Cellpose.

* Configurable BigStream using a YAML file

## 47686d8 - 08/05/2024

* Use a single Dask cluster to run all registrations

* Run round registration using Bigstream

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v1.0dev - [date]

Initial release of JaneliaSciComp/easifish, created with the [nf-core](https://nf-co.re/) template.

### `Added`

### `Fixed`

### `Dependencies`

### `Deprecated`
