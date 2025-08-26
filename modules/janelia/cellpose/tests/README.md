The test can be run using nf-core command:

`nf-core modules test cellpose`

or using:
```bash
nextflow run ./modules/janelia/cellpose/tests/main.nf -entry test_cellpose_standalone -c ./tests/config/nf-test.config -c ./modules/janelia/cellpose/tests/nextflow.config -profile podman
```

on an M1 Mac use:
```bash
nextflow run ./modules/janelia/cellpose/tests/main.nf -entry test_cellpose_standalone -c ./tests/config/nf-test.config -c ./modules/janelia/cellpose/tests/nextflow.config -profile podman --runtime_opts "--platform linux/arm64"
```
