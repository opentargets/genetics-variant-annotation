vcf=$1
chr=$2
maf=$3
vep=$4
outbuild=$5

if [ "$outbuild" -eq 37 ]; then out="${vcf}_MAF${maf}_Annotated_b37.vcf"; elif [ "$outbuild" -eq 38 ]; then out="./Temp/Chr${chr}/${vcf}_MAF${maf}_Annotated_b37.vcf"; fi
${vep}/vep --dir_cache ${vep} --cache_version 91 --cache -a GRCh37 --port 3337 -i ./Temp/Chr${chr}/${vcf}_MAF${maf}.vcf --format vcf --variant_class --regulatory --biotype --no_stats --vcf -o ${out} --force_overwrite --vcf_info_field VEP
if [ "$outbuild" -eq 37 ]; then gzip ${vcf}_MAF${maf}_Annotated_b37.vcf; elif [ "$outbuild" -eq 38 ]; then gzip ./Temp/Chr${chr}/${vcf}_MAF${maf}_Annotated_b37.vcf; fi

