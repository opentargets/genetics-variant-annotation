Variant annotation pipeline
===========================

Workflow to produce variant index for Open Targets Genetics.

Steps:
- Filters to remove low frequency variants (keep variant if MAF > 0.1% in any population)
- Removes variants that fail [hard or soft filters](https://macarthurlab.org/2018/10/17/gnomad-v2-1/)
- Lifts over to GRCh37
- Keep VEP annotations including regulatory and motif features

## Usage

```
usage: generate_variant_annotation.py [-h] [--gnomadFile GNOMADFILE] [--chainFile CHAINFILE] [--mafThreshold MAFTHRESHOLD] [--test TEST]
                                      --outputFolder OUTPUTFOLDER


This script generates variant table for OpenTargets Genetics pipelines.

optional arguments:
  -h, --help            show this help message and exit
  --gnomadFile GNOMADFILE
                        Hail table of the 3+ version of gnomAD dataset.
  --chainFile CHAINFILE
                        GRCh38 -> GRCh37 liftover chain file.
  --mafThreshold MAFTHRESHOLD
                        Lower threshold for minor allele frequency.
  --outputFolder OUTPUTFOLDER
                        Directory into which the output files will be saved.
  --test TEST           Number of rows taken for testing and debug purposes
```

## Start dataproc spark server
```
gcloud dataproc clusters create hail-cluster \
    --image-version=2.0.6-debian10 \
    --properties="^|||^spark:spark.task.maxFailures=20|||spark:spark.driver.extraJavaOptions=-Xss4M|||spark:spark.executor.extraJavaOptions=-Xss4M|||spark:spark.speculation=true|||hdfs:dfs.replication=1|||dataproc:dataproc.logging.stackdriver.enable=false|||dataproc:dataproc.monitoring.stackdriver.enable=false|||spark:spark.driver.memory=1146g" \
    --initialization-actions="gs://hail-common/hailctl/dataproc/0.2.73/init_notebook.py" \
    --metadata="^|||^WHEEL=gs://hail-common/hailctl/dataproc/0.2.73/hail-0.2.73-py3-none-any.whl|||PKGS=aiohttp==3.7.4|aiohttp_session>=2.7,<2.8|asyncinit>=0.2.4,<0.3|bokeh>1.3,<2.0|boto3>=1.17,<2.0|botocore>=1.20,<2.0|decorator<5|Deprecated>=1.2.10,<1.3|dill>=0.3.1.1,<0.4|gcsfs==0.8.0|fsspec==0.9.0|humanize==1.0.0|hurry.filesize==0.9|janus>=0.6,<0.7|nest_asyncio|numpy<2|pandas>=1.1.0,<1.1.5|parsimonious<0.9|PyJWT|python-json-logger==0.1.11|requests==2.25.1|scipy>1.2,<1.7|tabulate==0.8.3|tqdm==4.42.1|google-cloud-storage==1.25.*" \
    --master-machine-type=n1-standard-16 \
    --master-boot-disk-size=100GB \
    --num-master-local-ssds=0 \
    --region=europe-west1 \
    --initialization-action-timeout=20m \
    --project=open-targets-genetics-dev \
    --num-workers 2 \
    --worker-machine-type n1-standard-16 \
    --worker-boot-disk-size 500 \
    --labels=creator=dsuveges_ebi_ac_uk
```

## Sumbit job to dataproc server
```
# Get lastest hash
gcloud dataproc jobs submit pyspark   \
    --cluster=hail-cluster \
    --project=open-targets-genetics-dev \
    --region=europe-west1 generate_variant_annotation.py \
    -- \
    --outputFolder ${OUTPUT_BUCKET}
```

## Output schema
```
----------------------------------------
Global fields:
    None
----------------------------------------
Row fields:
    'locus': locus<GRCh37>
    'alleles': array<str>
    'locus_GRCh38': locus<GRCh38>
    'chrom_b37': str
    'pos_b37': int32
    'chrom_b38': str
    'pos_b38': int32
    'ref': str
    'alt': str
    'allele_type': str
    'vep': struct {
        most_severe_consequence: str,
        motif_feature_consequences: array<struct {
            allele_num: int32,
            consequence_terms: array<str>,
            high_inf_pos: str,
            impact: str,
            minimised: int32,
            motif_feature_id: str,
            motif_name: str,
            motif_pos: int32,
            motif_score_change: float64,
            strand: int32,
            variant_allele: str
        }>,
        regulatory_feature_consequences: array<struct {
            allele_num: int32,
            biotype: str,
            consequence_terms: array<str>,
            impact: str,
            minimised: int32,
            regulatory_feature_id: str,
            variant_allele: str
        }>,
        transcript_consequences: array<struct {
            allele_num: int32,
            amino_acids: str,
            biotype: str,
            canonical: int32,
            ccds: str,
            cdna_start: int32,
            cdna_end: int32,
            cds_end: int32,
            cds_start: int32,
            codons: str,
            consequence_terms: array<str>,
            distance: int32,
            domains: array<struct {
                db: str,
                name: str
            }>,
            exon: str,
            gene_id: str,
            gene_pheno: int32,
            gene_symbol: str,
            gene_symbol_source: str,
            hgnc_id: str,
            hgvsc: str,
            hgvsp: str,
            hgvs_offset: int32,
            impact: str,
            intron: str,
            lof: str,
            lof_flags: str,
            lof_filter: str,
            lof_info: str,
            minimised: int32,
            polyphen_prediction: str,
            polyphen_score: float64,
            protein_end: int32,
            protein_start: int32,
            protein_id: str,
            sift_prediction: str,
            sift_score: float64,
            strand: int32,
            swissprot: str,
            transcript_id: str,
            trembl: str,
            uniparc: str,
            variant_allele: str
        }>
    }
    'rsid': array<str>
    'af': struct {
        afr: float64,
        amr: float64,
        ami: float64,
        asj: float64,
        eas: float64,
        fin: float64,
        nfe: float64,
        mid: float64,
        sas: float64,
        oth: float64
    }
    'cadd': struct {
        raw: float64,
        phred: float64
    }
----------------------------------------
Key: ['locus', 'alleles']
----------------------------------------
```
