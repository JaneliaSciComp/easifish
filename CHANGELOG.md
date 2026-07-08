# JaneliaSciComp/easifish: Changelog
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.0.0 - 2026-07-08

### Added

- Added support for zarr v3

### Changed

- Renamed the skip_<step> parameters with run_<step> - it is less confusing

- Made the pipeline config files compliant with V2 parser

- Use templates for dask modules so that spark and dask workflows could also be executed with conda not only with a container runtime.


## 69f79cd - 2026-04-24

### Added

- Spots warping using Bigstream

- FISHSPOT module for spot extraction

- RS-FISH module for spot extraction

- Spots count and spots intensities

- Run volume segmentation using distributed Cellpose.

- Configurable BigStream using a YAML file

### Changed

- Configurable BigStitcher steps through a YAML file

- Option to separate the segmentation label merge for distributed cellpose

- Support spot extraction from warped image without the need to warp the spots

## 47686d8 - 2024-08-05

- Use a single Dask cluster to run all registrations

- Run round registration using Bigstream
