today=$(date)
vcf=$1
chr=$2
maf=$3
bin=$4
gnomad_version=$5


sed "s/{TODAY}/${today}/g" ${bin}/Subscripts/VCF_Header_Template.txt | sed "s/{GNOMAD_VERSION}/${gnomad_version}/g" > ./Temp/Chr${chr}/VCF_Header_chr${chr}.txt
cat ./Temp/Chr${chr}/${vcf}_MAF${maf}_VCFlike.txt >> ./Temp/Chr${chr}/VCF_Header_chr${chr}.txt
mv ./Temp/Chr${chr}/VCF_Header_chr${chr}.txt ./Temp/Chr${chr}/${vcf}_MAF${maf}.vcf
