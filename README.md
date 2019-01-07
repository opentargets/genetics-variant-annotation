Variant annotation pipeline
===========================

Workflow to produce variant index for Open Targets Genetics.

Steps:
- Filters to remove low frequency variants (MAF < 0.001% in any population)
- Removes variants that fail [hard or soft filters](https://macarthurlab.org/2018/10/17/gnomad-v2-1/)
- Lifts over to GRCh38
- Adds CADD annotations
- Adds VEP annotations including regulatory and motif features

Info:
- Output located `gs://genetics-portal-staging/variant-annotation/{version}/variant-annotation.parquet`
- Runs in ~1 hour using below config
- 72,909,456 total variants remain

## Start dataproc spark server
```
# Start server
gcloud beta dataproc clusters create \
    em-cluster \
    --image-version=1.2-deb9 \
    --metadata=MINICONDA_VERSION=4.4.10,JAR=gs://hail-common/builds/0.2/jars/hail-0.2-07b91f4bd378-Spark-2.2.0.jar,ZIP=gs://hail-common/builds/0.2/python/hail-0.2-07b91f4bd378.zip \
    --properties=spark:spark.driver.memory=41g,spark:spark.driver.maxResultSize=0,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,hdfs:dfs.replication=1 \
    --initialization-actions=gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://hail-common/cloudtools/init_notebook1.py \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=100GB \
    --num-master-local-ssds=0 \
    --num-preemptible-workers=0 \
    --num-worker-local-ssds=0 \
    --num-workers=3 \
    --preemptible-worker-boot-disk-size=40GB \
    --worker-boot-disk-size=40 \
    --worker-machine-type=n1-standard-32 \
    --zone=europe-west1-b \
    --initialization-action-timeout=20m \
    --max-idle=15m
```

## Sumbit job to dataproc server
```
# Get lastest hash
HASH=$(gsutil cat gs://hail-common/builds/0.2/latest-hash/cloudtools-3-spark-2.2.0.txt)

# Submit to cluster
gcloud dataproc jobs submit pyspark \
  --cluster=em-cluster \
  --files=gs://hail-common/builds/0.2/jars/hail-0.2-$HASH-Spark-2.2.0.jar \
  --py-files=gs://hail-common/builds/0.2/python/hail-0.2-$HASH.zip \
  --properties="spark.driver.extraClassPath=./hail-0.2-$HASH-Spark-2.2.0.jar,spark.executor.extraClassPath=./hail-0.2-$HASH-Spark-2.2.0.jar" \
  generate_variant_annotation.py

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
    'rsid': str
    'af': struct {
        gnomad_afr: float64,
        gnomad_amr: float64,
        gnomad_asj: float64,
        gnomad_eas: float64,
        gnomad_fin: float64,
        gnomad_nfe: float64,
        gnomad_nfe_est: float64,
        gnomad_nfe_nwe: float64,
        gnomad_nfe_onf: float64,
        gnomad_nfe_seu: float64,
        gnomad_oth: float64
    }
    'cadd': struct {
        raw: float64,
        phred: float64
    }
----------------------------------------
Key: ['locus', 'alleles']
----------------------------------------
```

## Old

#### Run locally
```
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
python generate_variant_annotation.py
```

#### Smaller cluster config
```
# Create command using Neale labs cloudtools
# cluster start --max-idle 15m --zone europe-west1-b --dry-run em-cluster

# Create server
gcloud beta dataproc clusters create \
    em-cluster \
    --image-version=1.2-deb9 \
    --metadata=MINICONDA_VERSION=4.4.10,JAR=gs://hail-common/builds/0.2/jars/hail-0.2-07b91f4bd378-Spark-2.2.0.jar,ZIP=gs://hail-common/builds/0.2/python/hail-0.2-07b91f4bd378.zip \
    --properties=spark:spark.driver.memory=41g,spark:spark.driver.maxResultSize=0,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,hdfs:dfs.replication=1 \
    --initialization-actions=gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://hail-common/cloudtools/init_notebook1.py \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=100GB \
    --num-master-local-ssds=0 \
    --num-preemptible-workers=0 \
    --num-worker-local-ssds=0 \
    --num-workers=2 \
    --preemptible-worker-boot-disk-size=40GB \
    --worker-boot-disk-size=40 \
    --worker-machine-type=n1-standard-8 \
    --zone=europe-west1-b \
    --initialization-action-timeout=20m \
    --max-idle=15m
```
