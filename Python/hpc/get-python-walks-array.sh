#!/bin/bash
#SBATCH -J [insert job name]
#SBATCH -N 1
#SBATCH --cpus-per-task 1
#SBATCH -t 3:00:00
#SBATCH --export=ALL
#SBATCH --mem=200GB
#SBATCH --mail-type=BEGIN,COMPLETED
#SBATCH --mail-user=insert@emailhere.com
#SBATCH --output=/insert/log/directory/here/SLURM-%u-%x-%j-tract%a.log 
#SBATCH --array=[insert-array-range]

# Create nested directory for a date if it doesn't exist
if [ ! -d /insert/log/directory/here/logs/$(date +%F)/${SLURM_JOB_NAME} ]; then
        mkdir -p /insert/log/directory/here/logs/$(date +%F)/${SLURM_JOB_NAME};
fi

# Print out Job Details
echo "Job ID: "$SLURM_JOB_ID
echo "Job Account: "$SLURM_JOB_ACCOUNT
echo "Hosts: "$SLURM_NODELIST
echo "------------"

# Activate Python virtual work environment
source /python/virtual/environment/py_venv/bin/activate

# Run code and save to nested dir
python3 /insert/file/location/get_walks.py $SLURM_ARRAY_TASK_ID &> /insert/log/directory/here/logs/$(date +%F)/${SLURM_JOB_NAME}/${USER}-${SLURM_JOB_NAME}-${SLURM_JOB_ID}-tract${SLURM_ARRAY_TASK_ID}.log

exit_status=$?

if [[ $exit_status -ne 0 ]]; then
	echo "failed"
else
	echo "success"
fi

echo "Job complete."
