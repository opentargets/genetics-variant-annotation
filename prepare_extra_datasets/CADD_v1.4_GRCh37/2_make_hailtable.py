#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import hail as hl
import sys

def main():

    # Args
    inf = 'gs://genetics-portal-staging/variant-annotation/extra_datasets/CADD_v1.4_GRCh37/inputs/gnomad.genomes.r2.0.1.sites.tsv'
    outf = 'gs://genetics-portal-staging/variant-annotation/extra_datasets/CADD_v1.4_GRCh37/output/cadd_v1.4_gnomad.genomes.r2.0.1.sites.ht'

    # Load data
    ht = hl.import_table(
        inf,
        comment='#',
        no_header=True,
        min_partitions=128,
        types={
            'f0':'str',
            'f1':'int32',
            'f2':'str',
            'f3':'str',
            'f4':'float64',
            'f5':'float64'}
    )

    # Rename columns
    ht = ht.rename({
        'f0': 'chrom',
        'f1': 'pos',
        'f2': 'ref',
        'f3': 'alt',
        'f4': 'cadd_raw',
        'f5': 'cadd_phred',
    })

    # Create locus and allele
    ht = ht.annotate(
        locus=hl.locus(ht.chrom, ht.pos, 'GRCh37'),
        alleles=hl.array([ht.ref, ht.alt])
    )

    # Keep required
    ht = ht.select(ht.locus, ht.alleles, ht.cadd_raw, ht.cadd_phred)

    # Key by locus and alleles
    ht = ht.key_by(ht.locus, ht.alleles)

    # Repartition
    # For 260657874 rows, gives ~2 million rows (~50Mb) per parition.
    # ht = ht.repartition(128, shuffle=True)

    # Write
    ht.write(outf)

    return 0


if __name__ == '__main__':

    main()
