#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import hail as hl
import sys

def main():

    # Args
    version = '190129'
    hail_table = 'gs://gnomad-public/release/2.1/ht/genomes/gnomad.genomes.r2.1.sites.ht'
    chain_file = 'gs://hail-common/references/grch37_to_grch38.over.chain.gz'
    cadd_table = 'gs://genetics-portal-staging/variant-annotation/extra_datasets/CADD_v1.4_GRCh37/output/cadd_v1.4_gnomad.genomes.r2.0.1.sites.ht'
    out_parquet = 'gs://genetics-portal-staging/variant-annotation/{version}/variant-annotation.parquet'.format(version=version)
    out_sitelist = 'gs://genetics-portal-staging/variant-annotation/{version}/variant-annotation.sitelist.tsv.gz'.format(version=version)
    out_partitions = 256
    maf_filter = 0.001 # 0.1%

    # Local paths
    # hail_table = 'data/gnomad.genomes.head100k.r2.1.sites.ht'
    # chain_file = 'data/grch37_to_grch38.over.chain.gz'
    # cadd_table = 'prepare_extra_datasets/CADD_v1.4_GRCh37/temp/cadd_v1.4_gnomad.genomes.r2.0.1.sites.ht'
    # out_parquet = 'output/variant-annotation.parquet'

    # Check output doesn't exist
    if hl.utils.hadoop_exists(out_parquet):
        sys.exit('Error! Output file already exists: {0}'.format(out_parquet))

    #
    # Load ---------------------------------------------------------------------
    #

    # Load data
    ht = hl.read_table(hail_table)
    print('Total number of rows: ', ht.count())

    # DEBUG take head
    # ht = ht.head(10000)

    # Assert that all alleles are biallelic
    assert(ht.all(ht.alleles.length() == 2))

    #
    # Remove variants not passing hard or soft filters -------------------------
    # https://macarthurlab.org/2018/10/17/gnomad-v2-1/
    #

    print('Variants pre-filtering: ', ht.count())
    ht = ht.filter(ht.filters.length() == 0)
    print('Variants post-quality filter: ', ht.count())

    #
    # Filter based on allele frequency -----------------------------------------
    #

    # WARNING: Not sure how to do this dynamically, so it is hard coded.
    # May need updating for future versions of gnomad.

    # Print population keys that need to be included
    # populations = parse_population_keys(
    #     sorted(hl.eval(ht.globals.freq_index_dict.keys())) )
    # print(populations)

    ht = ht.annotate(af = hl.struct(), maf = hl.struct())
    ht = ht.annotate(

        # Allele freqs
        af = ht.af.annotate(
            gnomad_afr = ht.freq[ht.globals.freq_index_dict['gnomad_afr']].AF,
            gnomad_amr = ht.freq[ht.globals.freq_index_dict['gnomad_amr']].AF,
            gnomad_asj = ht.freq[ht.globals.freq_index_dict['gnomad_asj']].AF,
            gnomad_eas = ht.freq[ht.globals.freq_index_dict['gnomad_eas']].AF,
            gnomad_fin = ht.freq[ht.globals.freq_index_dict['gnomad_fin']].AF,
            gnomad_nfe = ht.freq[ht.globals.freq_index_dict['gnomad_nfe']].AF,
            gnomad_nfe_est = ht.freq[ht.globals.freq_index_dict['gnomad_nfe_est']].AF,
            gnomad_nfe_nwe = ht.freq[ht.globals.freq_index_dict['gnomad_nfe_nwe']].AF,
            gnomad_nfe_onf = ht.freq[ht.globals.freq_index_dict['gnomad_nfe_onf']].AF,
            gnomad_nfe_seu = ht.freq[ht.globals.freq_index_dict['gnomad_nfe_seu']].AF,
            gnomad_oth = ht.freq[ht.globals.freq_index_dict['gnomad_oth']].AF
        ),

        # Minor allele freq
        maf = ht.maf.annotate(
            gnomad_afr = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_afr']].AF),
            gnomad_amr = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_amr']].AF),
            gnomad_asj = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_asj']].AF),
            gnomad_eas = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_eas']].AF),
            gnomad_fin = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_fin']].AF),
            gnomad_nfe = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_nfe']].AF),
            gnomad_nfe_est = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_nfe_est']].AF),
            gnomad_nfe_nwe = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_nfe_nwe']].AF),
            gnomad_nfe_onf = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_nfe_onf']].AF),
            gnomad_nfe_seu = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_nfe_seu']].AF),
            gnomad_oth = af_to_maf(ht.freq[ht.globals.freq_index_dict['gnomad_oth']].AF)
        )

    )

    # Filter based on MAFs
    ht = ht.filter(
        (ht.maf.gnomad_afr >= maf_filter) |
        (ht.maf.gnomad_amr >= maf_filter) |
        (ht.maf.gnomad_asj >= maf_filter) |
        (ht.maf.gnomad_eas >= maf_filter) |
        (ht.maf.gnomad_fin >= maf_filter) |
        (ht.maf.gnomad_nfe >= maf_filter) |
        (ht.maf.gnomad_nfe_est >= maf_filter) |
        (ht.maf.gnomad_nfe_nwe >= maf_filter) |
        (ht.maf.gnomad_nfe_onf >= maf_filter) |
        (ht.maf.gnomad_nfe_seu >= maf_filter) |
        (ht.maf.gnomad_oth >= maf_filter)
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
    ht = ht.annotate(cadd = hl.struct())
    ht = ht.annotate(
        cadd = ht.cadd.annotate(
            raw = cadd[ht.locus, ht.alleles].cadd_raw,
            phred = cadd[ht.locus, ht.alleles].cadd_phred
        )
    )

    #
    # Export required fields ---------------------------------------------------
    #

    # Split locus and alleles into separate fields
    ht = ht.annotate(
        chrom_b37 = ht.locus.contig,
        pos_b37 = ht.locus.position,
        chrom_b38 = ht.locus_GRCh38.contig.replace('chr', ''),
        pos_b38 = ht.locus_GRCh38.position,
        ref = ht.alleles[0],
        alt = ht.alleles[1]
    )

    # Drop top level fields
    ht = ht.drop(
        # 'locus',
        # 'locus_GRCh38',
        # 'alleles',
        'freq',
        'age_hist_het',
        'age_hist_hom',
        'popmax',
        'faf',
        'lcr',
        'decoy',
        'segdup',
        'nonpar',
        'variant_type',
        # 'allele_type',
        'n_alt_alleles',
        'was_mixed',
        'has_star',
        'qd',
        'pab_max',
        'info_MQRankSum',
        'info_SOR',
        'info_InbreedingCoeff',
        'info_ReadPosRankSum',
        'info_FS',
        'info_QD',
        'info_MQ',
        'info_DP',
        'transmitted_singleton',
        'fail_hard_filters',
        'info_POSITIVE_TRAIN_SITE',
        'info_NEGATIVE_TRAIN_SITE',
        'omni',
        'mills',
        'tp',
        'rf_train',
        'rf_label',
        'rf_probability',
        'rf_prediction',
        'rank',
        'was_split',
        'singleton',
        '_score',
        '_singleton',
        'biallelic_rank',
        'singleton_rank',
        'n_nonref',
        'score',
        'adj_biallelic_singleton_rank',
        'adj_rank',
        'adj_biallelic_rank',
        'adj_singleton_rank',
        'biallelic_singleton_rank',
        'filters',
        'gq_hist_alt',
        'gq_hist_all',
        'dp_hist_alt',
        'dp_hist_all',
        'ab_hist_alt',
        'qual',
        'allele_info',
        'maf'
    )

    # Drop all globals
    ht = ht.select_globals()

    # Drop unnecessary VEP fields
    ht = ht.annotate(
        vep = ht.vep.drop(
            'assembly_name',
            'allele_string',
            'ancestral',
            'colocated_variants',
            'context',
            'end',
            'id',
            'input',
            'intergenic_consequences',
            'seq_region_name',
            'start',
            'strand',
            'variant_class'
        )
    )

    # Sort columns
    col_order = ['locus_GRCh38', 'chrom_b37', 'pos_b37', 'chrom_b38', 'pos_b38',
                 'ref', 'alt', 'allele_type', 'vep', 'rsid', 'af', 'cadd']
    ht = ht.select(*col_order)

    # Persist as writing twice would cause re-computation
    ht = ht.persist()

    # Repartition and write parquet file
    (
        ht.to_spark(flatten=False)
          .repartition(out_partitions)
          .write.parquet(out_parquet)
    )

    # Export site list
    cols = ['chrom_b37', 'pos_b37', 'chrom_b38', 'pos_b38', 'ref', 'alt', 'rsid']
    (
        ht.select(*cols)
          .export(out_sitelist)
    )

    return 0

def parse_population_keys(pop_list):
    ''' Takes a list of population names and filters to:
        - Keep gnomad only
        - Remove male/female sub-stats
        - Remove _raw (stats before any sample filtering)
    Params:
        pop_list (list of str)
    Returns:
        Filtered pop_list (list of str)
    '''
    pop_filt = []
    for pop in pop_list:
        if (
              pop.startswith('gnomad_') and not
              pop.endswith('_raw') and not
              pop.endswith('_male') and not
              pop.endswith('_female')
           ):
           pop_filt.append(pop)
    return pop_filt


def af_to_maf(af):
    ''' Convert allele freq to minor allele freq using hail conditional
    '''
    return hl.cond(af <= 0.5, af, 1 - af)

if __name__ == '__main__':

    main()
