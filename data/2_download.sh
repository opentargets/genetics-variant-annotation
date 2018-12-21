#!/usr/bin/env bash
#

set -euo pipefail

gsutil -m cp -r gs://genetics-portal-analysis/gnomad.genomes.head100k.r2.1.sites.ht .

echo COMPLETE
