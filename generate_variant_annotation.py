import argparse
import logging
import sys

import pyspark.sql.functions as f

import hail as hl
from hail.expr.functions import allele_type


# Gnomad hail table:
GNOMAD_3_TABLE = 'gs://gcp-public-data--gnomad/release/3.1.1/ht/genomes/gnomad.genomes.v3.1.1.sites.ht'

# GRCh38 to 37 chainfile:
CHAIN_FILE = 'gs://hail-common/references/grch38_to_grch37.over.chain.gz'

# Parameters:
OUT_PARTITIONS = 256

# Population of interest:
POPULATIONS = {
    'afr',  # African-American
    'amr',  # American Admixed/Latino
    'ami',  # Amish ancestry
    'asj',  # Ashkenazi Jewish
    'eas',  # East Asian
    'fin',  # Finnish
    'nfe',  # Non-Finnish European
    'mid',  # Middle Eastern
    'sas',  # South Asian
    'oth'   # Other
}

def main(gnomad_file, chain_file, out_parquet, test=None):

    # Load data
    ht = hl.read_table(gnomad_file)

    # If process is being tested, take head:
    if test:
        ht = ht.head(test)

    # Assert that all alleles are biallelic:
    assert ht.all(ht.alleles.length() == 2), 'Mono- or multiallelic variants have been found.'

    # Extracting AF indices of populations:
    population_indices = ht.globals.freq_index_dict.collect()[0]
    population_indices = {pop: population_indices[f'{pop}-adj'] for pop in POPULATIONS}

    # Generate struct for alt. allele frequency in selected populations:
    ht = ht.annotate(af=hl.struct(**{pop: ht.freq[index].AF for pop, index in population_indices.items()}))

    # Add chain file
    grch37 = hl.get_reference('GRCh37')
    grch38 = hl.get_reference('GRCh38')
    grch38.add_liftover(chain_file, grch37)

    # Liftover
    ht = ht.annotate(
        locus_GRCh37=hl.liftover(ht.locus, 'GRCh37')
    )

    # Adding build-specific coordinates to the table:
    ht = ht.annotate(
        chrom_b38=ht.locus.contig.replace('chr', ''),
        pos_b38=ht.locus.position,
        chrom_b37=ht.locus_GRCh37.contig.replace('chr', ''),
        pos_b37=ht.locus_GRCh37.position,
        ref=ht.alleles[0],
        alt=ht.alleles[1],
        allele_type=ht.allele_info.allele_type
    )

    # Updating table:
    ht = ht.annotate(
        # Updating CADD column:
        cadd=ht.cadd.rename({'raw_score': 'raw'}).drop('has_duplicate'),

        # Adding locus as new column:
        locus_GRCh38=ht.locus
    )

    # Drop all global annotations:
    ht = ht.select_globals()

    # Drop unnecessary VEP fields
    ht = ht.annotate(
        vep=ht.vep.drop(
            'assembly_name',
            'allele_string',
            'ancestral',
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
    col_order = [
        'locus_GRCh38', 'chrom_b38', 'pos_b38',
        'chrom_b37', 'pos_b37',
        'ref', 'alt', 'allele_type', 'vep', 'rsid', 'af', 'cadd', 'filters'
    ]

    # Convert data:
    (
        # Select columns and convert to pyspark:
        ht.select(*col_order).to_spark(flatten=False)

        # Creating new column based on the transcript_consequences
        .select("*", f.expr("filter(vep.transcript_consequences, array -> array.canonical == True)").alias("transcript_consequences"))

        # Re-creating the vep column with the new transcript consequence object:
        .withColumn(
            'vep',
            f.struct(
                f.col('vep.most_severe_consequence').alias('most_severe_consequence'),
                f.col('vep.motif_feature_consequences').alias('motif_feature_consequences'),
                f.col('vep.regulatory_feature_consequences').alias('regulatory_feature_consequences'),
                f.col('transcript_consequences').alias('transcript_consequences')
            )
        )

        # Drop unused column:
        .drop('transcript_consequences')

        # Adding new column:
        .withColumn('chr', f.col('chrom_b38'))

        # Writing data partitioned by chromosome:
        .write.mode('overwrite')
        .partitionBy('chr')
        .parquet(out_parquet)
    )

if __name__ == '__main__':

    # Parsing command line arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='This script generates variant table for OpenTargets Genetics pipelines.')
    parser.add_argument('--gnomadFile', help='Hail table of the 3+ version of gnomAD dataset.',
                        type=str, required=False, default=GNOMAD_3_TABLE)
    parser.add_argument('--chainFile', help='GRCh38 -> GRCh37 liftover chain file.',
                        type=str, required=False, default=CHAIN_FILE)
    parser.add_argument('--outputFolder', help='Directory into which the output files will be saved.',
                        type=str, required=True)
    parser.add_argument('--test', help='Number of rows taken for testing and debug purposes',
                        type=int, required=False)

    args = parser.parse_args()

    gnomad_file = args.gnomadFile if args.gnomadFile else GNOMAD_3_TABLE
    chain_file = args.chainFile if args.chainFile else CHAIN_FILE
    out_folder = args.outputFolder

    # Initialize logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr
    )

    # Report parametrs
    logging.info(f'GnomAD file: {gnomad_file}')
    logging.info(f'Chains file: {chain_file}')
    logging.info(f'Chain file: {chain_file}')
    logging.info(f'Output folder: {out_folder}')
    if args.test:
        logging.info(f'Test run. Number of variants taken: {args.test}')

    main(gnomad_file, chain_file, out_folder, args.test)
