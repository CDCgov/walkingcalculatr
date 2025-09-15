#!/bin/bash
#!/usr/bin/python
#SBATCH --job-name=[insert name here]
#SBATCH --nodes=1
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=10
#SBATCH --gpus-per-node=0
#SBATCH --output=/insert/log/directory/here/logs/%u-%x-%j.log
#SBATCH --export=ALL
#SBATCH --mem=240GB
#SBATCH --export=ALL

echo "Job ID: "$SLURM_JOB_ID
echo "Job Account: "$SLURM_JOB_ACCOUNT
echo "Hosts: "$SLURM_NODELISTs
echo "Arguments: ${@}"
echo "Running python script create-stratify.py"

# Activate Python work environment before running the py file
source /insert/virtual/environment/here/py_env/bin/activate

python3 /insert/directory/of/file/here/create-stratify.py "${@}" &> /insert/log/directory/here/logs/${USER}-${SLURM_JOB_NAME}-${SLURM_JOB_ID}.log

exit_status=$?

if [[ $exit_status -ne 0 ]]; then
	echo "failed"
else
	echo "success"
fi
