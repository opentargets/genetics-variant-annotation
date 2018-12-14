module load hgi/systems/jdk/latest
module list

chr=$1
maf=$2
outbuild=$3

vcfdir="/lustre/scratch115/projects/otcoregen/dw16/gnomad"
vcf="gnomad.genomes.r2.1.sites.chr${chr}"

snpsift="/lustre/scratch115/projects/otcoregen/reference_data/snpEff/SnpSift.jar"
vep="/lustre/scratch115/projects/otcoregen/reference_data/ensembl-vep"
bin="/lustre/scratch115/projects/otcoregen/dw16/gnomad/Annotate_VCF"

chaindir="/lustre/scratch115/projects/otcoregen/reference_data/Chain_Files"
fasta="${chaindir}/Homo_sapiens.GRCh38.dna.alt.fa.gz"
chain="${chaindir}/GRCh37_to_GRCh38.chain.gz"

gnomad_ver=1

# Check specified output build
echo "Assumed input build is 37"
case ${outbuild} in
        37)
                echo "Build in output VCF will be 37"
                ;;
        38)
                echo "Build in output VCF will be 38 (Crossmap)"
                ;;
        *)
                echo "${outbuild} is not a valid output build"
                echo "Specify either 37 or 38 as the output build"
                exit 1
                ;;
esac

mkdir -p ./Temp/Chr${chr}
mkdir -p ./Logs

echo "Running annotation for chromosome ${chr} (${vcfdir}/${vcf})"
echo "Variants will be filtered to MAF >= ${maf}"

#bsub -J FromVCF_chr${chr} -o ./Temp/Chr${chr}/${vcf}.txt -R"select[mem>15000] rusage[mem=15000]" -M15000 java -jar ${snpsift} extractFields ${vcfdir}/${vcf}.vcf.gz CHROM POS ID REF ALT controls_AF_afr controls_AF_eas controls_AF_amr controls_AF_nfe
#bsub -J FromVCF_chr${chr} -o ./Logs/Sleep_chr${chr}.log sleep 5
#bsub -w "done(FromVCF_chr${chr})" -J FilterVar_chr${chr} -o ./Logs/FilterVar_chr${chr}.log -R"select[mem>5000] rusage[mem=5000]" -M5000 python2.7 ${bin}/Subscripts/Drop_MAF.py ${vcf} ${chr} ${maf}
#bsub -w "done(FilterVar_chr${chr})" -J ConstructVCF_chr${chr} -o ./Logs/ConstructVCF_chr${chr}.log bash ${bin}/Subscripts/Construct_VCF.sh ${vcf} ${chr} ${maf} ${bin} ${gnomad_ver}
#bsub -w "done(ConstructVCF_chr${chr})" -J VEP_chr${chr} -o ./Logs/VEP_chr${chr}.log -R"select[mem>5000] rusage[mem=5000]" -M5000 bash ${bin}/Subscripts/VEP_Run.sh ${vcf} ${chr} ${maf} ${vep} ${outbuild}

if [ "${outbuild}" -eq 38 ]; then
	bsub -w "done(VEP_chr${chr})" -J ConvertCoords_chr${chr} -o ./Logs/ConvertCoords_chr${chr}.log bash ${bin}/Subscripts/Crossmap_Run.sh ${chr} ${maf} ${vcf} ${chain} ${fasta}
	bsub -w "done(ConvertCoords_chr${chr})" -J Cleanup_chr${chr} -o ./Logs/Cleanup_chr${chr}.log rm -r ./Temp/Chr${chr}
elif [ "${outbuild}" -eq 37 ]; then
	bsub -w "done(VEP_chr${chr})" -J Cleanup_chr${chr} -o ./Logs/Cleanup_chr${chr}.log rm -r ./Temp/Chr${chr}
fi
