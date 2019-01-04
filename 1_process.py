#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import hail as hl
import sys

def main():

    # Args
    hail_table = 'data/gnomad.genomes.head100k.r2.1.sites.ht'
    chain_file = 'data/grch37_to_grch38.over.chain.gz'
    cadd_table = 'prepare_extra_datasets/CADD_v1.4_GRCh37/temp/cadd_v1.4_gnomad.genomes.r2.0.1.sites.ht' # genetics-portal-staging/variant-annotation/extra_datasets/CADD_v1.4_GRCh37/output/cadd_v1.4_gnomad.genomes.r2.0.1.sites.ht
    populations = ['controls_afr', 'controls_amr', 'controls_eas',
                   'controls_nfe']
    maf_filter = 0.001 # 0.1%
    # maf_filter = 0

    #
    # Load ---------------------------------------------------------------------
    #

    # Load data
    ht = (
        hl.read_table(hail_table)
          .head(100) # DEBUG
    )

    # Assert that all alleles are biallelic
    assert(ht.all(ht.alleles.length() == 2))

    #
    # Remove variants not passing hard or soft filters -------------------------
    # https://macarthurlab.org/2018/10/17/gnomad-v2-1/
    #

    print('Variants pre-quality filter: ', ht.count())
    ht = ht.filter(ht.filters.length() == 0)
    print('Variants post-quality filter: ', ht.count())

    #
    # Filter based on allele frequency -----------------------------------------
    #

    # Calc minor allele freqs
    for pop in populations:
        ht = ( ht.annotate(pop_maf = af_to_maf(
                                ht.freq[ht.globals.freq_index_dict[pop]].AF))
                 .rename({'pop_maf': '{pop}_maf'.format(pop=pop)}) )

    # Filter based on MAFs. Not sure how to do this dynamically
    print('Variants pre-MAF filter: ', ht.count())
    ht = ht.filter(
        (ht.controls_afr_maf >= maf_filter) |
        (ht.controls_amr_maf >= maf_filter) |
        (ht.controls_eas_maf >= maf_filter) |
        (ht.controls_nfe_maf >= maf_filter)
    )
    print('Variants post-MAF filter: ', ht.count())

    #
    # Lift-over to GRCh38 ------------------------------------------------------
    #

    # Add chain file
    rg37 = hl.get_reference('GRCh37')
    rg38 = hl.get_reference('GRCh38')
    rg37.add_liftover(chain_file, rg38)

    # Liftover
    ht = ht.annotate(
        locus_GRCh38 = hl.liftover(ht.locus, 'GRCh38')
    )

    #
    # Add CADD annotations -----------------------------------------------------
    #

    # Load CADD annotations
    cadd = hl.read_table(cadd_table)

    # Annotate gnomad with CADD
    ht = ht.annotate(
        cadd_raw = cadd[ht.locus, ht.alleles].cadd_raw,
        cadd_phred = cadd[ht.locus, ht.alleles].cadd_phred
    )

    #
    # Export required fields ---------------------------------------------------
    #

    # Write example TSV
    ht.export('output/example.tsv',
              types_file='output/example.types.txt',
              header=True, parallel=None)

    return 0

def af_to_maf(af):
    ''' Convert allele freq to minor allele freq using hail conditional
    '''
    return hl.cond(af <= 0.5, af, 1 - af)

if __name__ == '__main__':

    main()
