import numpy as np
import pandas as pd
import sys
pd.options.mode.chained_assignment = None 

# Grab options passed at terminal
vcf = sys.argv[1]
chr = sys.argv[2]
maf = float(sys.argv[3])
##

filein = "./Temp/Chr" + chr + "/" + vcf + ".txt"
data = pd.read_csv(filein,sep='\t')

print "Read {} variants".format(len(data))

# Currently excludes ASJ (Ashkenazi Jews), FIN (Finnish) and 'Other'.  
# SAS is not available in the downloaded file
populations = ['afr','amr','eas','nfe']

# Get MAF in each specified population
for pop in populations:
	orig_varname = 'controls_AF_' + pop
	data['flipped']=1-data[orig_varname]
	maf_varname = 'MAF_' + pop
	data[maf_varname] = data[[orig_varname,'flipped']].min(axis=1)
del data['flipped']

# Retain if MAF is >= threshold in at least one population
maf_filtered = data[((data['MAF_afr']>=maf) | (data['MAF_amr']>=maf) | (data['MAF_eas']>=maf) | (data['MAF_nfe']>=maf))]
maf_filtered['MAF_highest'] = maf_filtered[['MAF_afr','MAF_amr','MAF_eas','MAF_nfe']].max(axis=1)
print "{} variants have MAF >= {}".format(len(maf_filtered),maf)
print maf_filtered.head()

# Generate chr_pos ID
maf_filtered['POS'] = maf_filtered['POS'].astype(int)
maf_filtered['varid_b37'] = maf_filtered['CHROM'].map(str) + '_' + maf_filtered['POS'].map(str) + '_' + maf_filtered['REF'] + '_' + maf_filtered['ALT']

# Format to VCF-like, retain originally-reported ALT AF
maf_filtered['INFO'] = 'AF_afr='+maf_filtered['controls_AF_afr'].map(str)+';AF_amr='+maf_filtered['controls_AF_amr'].map(str)+';AF_nfe='+maf_filtered['controls_AF_nfe'].map(str)+';AF_eas='+maf_filtered['controls_AF_eas'].map(str)+';rsid='+maf_filtered['ID']
maf_filtered['QUAL'] = np.nan
maf_filtered['FILTER'] = np.nan
vcf_fields = maf_filtered[['CHROM','POS','varid_b37','REF','ALT','QUAL','FILTER','INFO']]
vcf_fields.rename(columns={'varid_b37':'ID'}, inplace=True)
print vcf_fields.head()

# Output as VCF table without header
fileout = "./Temp/Chr" + chr + "/" + vcf + '_MAF' + str(maf) + '_VCFlike.txt'
vcf_fields.to_csv(fileout, sep='\t', index=False, header=False)
