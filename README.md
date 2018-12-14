###

Pipeline to Annotate GNOMAD Variants with VEP Consequences and Regulatory Elements
Dan Wright, December 2018

###

Pipeline runs on Sanger Farm3
Built for Gnomad variants on hg19/GRCh37
If Gnomad changes to build 38, changes will need to be made to VEP_Run.sh, to update the cache location.  The assembly conversion step will also need to be deactivated in the wrapper script
NOTE:  The wrapper runs smoothly up until the production of the annotated VCF.  Accessing Crossmap is sometimes problematic, so ensuring it is installed properly is important
If Crossmap becomes problematic, the assembly conversion step can be manually deactivated in the wrapper script by commenting out.

## Required Modules
- Activated Python environment with pandas, numpy and sys
- Latest JDK (script loads latest version from HGI Modules as default)
- SNPEff/SNPSift .jar
- Crossmap loaded into Python environment


## Before Running
- Download the required Gnomad chromosome files in Build 37 to chosen location (access via http://gnomad.broadinstitute.org/downloads as of December 2018).  
  If renaming the downloaded VCFs, stick to a standardised formula which contains the chromosome number and redefine the $vcf variable - see below
- Change the extension for each file from .vcf.bgz to .vcf.gz (use mv) 
- Activate the Python environment containing the dependencies specified above
- Check that fixed variables at the start of the wrapper script are correctly specified
	- Change the naming convention of downloaded VCFs if required ($vcf)
	- Specify where downloaded Gnomad VCFs are located ($vcfdir)
	- Specify the location of the folder conatining the wrapper script ($bin)
	- Specify the location of the directory containing the VEP executable ($vep)
	- Specify the location of the SnpSift .jar file ($snpsift)
	- Specify the Gnomad version being used ($gnomadver)
	- Specify the location of the chain file for assembly conversion, downloadable from Ensembl FTP ($chaindir)
	- Specify the name of the 37>38 chain file within that directory ($chain)


## To Run
Run the wrapper script from the directory where you want results stored, passing the positional parameters
	1)  Chrosome number to annotate
	2)  Minimum MAF for frequency filtering (e.g. 0.01)
	3)  The required output build (37 or 38)
Ensure that the hardcoded paths to SnpSift, VEP, the pipeline subscripts etc. are correct in the wrapper script  


## Pseudocode
Each part ('subscript') of the annotation pipeline is coded separately in Bash, SnpSift (Java), or Python2.7
The main shell script is a wrapper which schedules each module as a job on Farm3, and sets the dependencies between each 
Log files for every subscript are saved to ./Logs
The pipeline generates a number of temporary files in ./Temp/Chr${chr}.  This directory is deleted automatically once the output is produced.
If the pipeline fails before the successful production of the annotated VCFs, all temp files are retained for troubleshooting

INPUT:  VCF of Gnomad variants, as downloaded from Gnomad server

	1) Extract alternate allele frequencies (controls) from Gnomad VCF and output as TSV (SnpSift)
		- Useful step to reduce burden of data IO later; Gnomad VCFs are very large and have more than 600 INFO elements
	2) Use AFs to infer MAF and restrict to the specified frequency threshold (Python2.7)
	3) Format TSV to VCF-like table with VCF variable names, and AFs in INFO field (Python2.7)
	4) Construct custom VCF header and prepend to VCF-like table; output as VCF (Bash)
	5) Annotate VCF with consequences and regulatory elements in VEP; output as VCF (Ensembl VEP from offline cache, Perl)
	6) If output build specified is 38, run liftover of b37 VCF from step (5); output as VCF with CHROM and POS on b38 

OUTPUT:  VCF of frequency-filtered variants annotated with functional consequences and regulatory elements (VEP subelement of INFO field) saved to launch directory
         Details of each annotation are provided in the VCF header
	 If output build specified as 37, output VCF is on b37 throughout
	 If output build specified as 38, output VCF CHROM and POS fields are on b38, ID contains the original b37 position and the alleles 


## Testing and Time Complexity
The shell script and memory specifications work well even for the largest Gnomad chromosomes
Smallest chromosomes take ~1hr to run, largest ~3hrs


## Future Developments
- Modify so that Gnomad VCF does not need to be downloaded initially, but is streamed straight to SnpSift
   

