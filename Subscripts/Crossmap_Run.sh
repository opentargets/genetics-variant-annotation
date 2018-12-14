chr=$1
maf=$2
vcf=$3
chain=$4
fasta=$5

python2.7 CrossMap.py vcf ${chain} ./Temp/Chr${chr}/${vcf}_MAF${maf}_Annotated_b37.vcf.gz ${fasta} ${vcf}_MAF${maf}_Annotated_b38.vcf
gzip ${vcf}_Annotated_b38.vcf
