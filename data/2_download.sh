#!/usr/bin/env bash
#

set -euo pipefail

gsutil -m cp -r gs://genetics-portal-analysis/gnomad.genomes.head100k.r2.1.sites.ht .
gsutil -m cp -r gs://hail-common/references/grch37_to_grch38.over.chain.gz .

echo COMPLETE
