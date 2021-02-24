#!/bin/bash

##################################################################################################################################################################
#	
#	File : EXEC WORKFLOW CTR
#	Autor: Bruno William
#	Date : 22/02/2021
#
##################################################################################################################################################################

###############################################################################################################################################################
Carga_Config ()
{
	>$FICH_CFG

	cat $FICH_SCRIPT_CFG | grep -n : | cut -d: -f3-100 | while read variable
	do
		echo $variable >> $FICH_CFG
	done
}
##################################################################################################################################################################

##################################################################################################################################################################
#	Programa.	#
##################################################################################################################################################################
source /etc/profile

export ARG1=$1

FICH_SCRIPT_CFG=/projetoBase/parameters/arquivoConfiguracao.cfg
FICH_CFG=/projetoBase/parameters/Carga_Variable_arquivoConfiguracao.cfg

dos2unix $FICH_SCRIPT_CFG 2>>/dev/null
dos2unix $FICH_CFG 2>>/dev/null

`Carga_Config`
. $FICH_CFG

##################################################################################################################################################################
#	START ENGINE   #
##################################################################################################################################################################


echo "   "
echo "   "
echo "   "
echo "          ARG1 ::: "''$ARG1
echo "   "
echo "   " 
echo "   " 

### SPARK SUBMIT
spark-submit --master yarn-cluster --deploy-mode cluster --driver-memory $DRIVERMER --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEMORY \
--conf spark.app.name=$JOBNAME"_"$APPNAME"_"$ODATE \
--conf spark.dynamicAllocattion.enable=True \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.yarn.driver.memoryOverhead=4096 \
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:ParallelGCThreads=20 -XX:ConcGCThreads=20 -XX:InitiatingHeapOccupancyPercent=70" \
--files /projetoBase/csvFiles/product.csv,/projetoBase/csvFiles/tax.csv,/projetoBase/csvFiles/vol_2019.csv.gz \
--py-files /projetoBase/scripts/codigos.egg \
$PYFILES \
$ARG1