#!/usr/bin/env bash
#

set -euo pipefail

wget https://krishna.gs.washington.edu/download/CADD/v1.4/GRCh37/gnomad.genomes.r2.0.1.sites.tsv.gz
gunzip gnomad.genomes.r2.0.1.sites.tsv.gz
gsutil -m cp gnomad.genomes.r2.0.1.sites.tsv gs://genetics-portal-staging/variant-annotation/extra_datasets/CADD_v1.4_GRCh37/inputs/gnomad.genomes.r2.0.1.sites.tsv
rm gnomad.genomes.r2.0.1.sites.tsv

echo COMPLETE
