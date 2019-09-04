Experiment log (oldest first)

# isilon-rep1, 92 jobs

* Removed UPSM_1 subjects manually from bosh-commands.sh because datalad
  wouldn't download their files.
* Obtained 61% of failed tasks
* By site: Counter({'SWU_4': 235, 'NYU_2': 187, 'DC_1': 114, 'IPCAS_7': 74,
'BNU_2': 61, 'MRN': 55, 'LMU_2': 40, 'IBA_TRT': 36, 'IPCAS_2': 35, 'LMU_1':
27, 'XHCUMS': 25, 'NKI_TRT': 24, 'SWU_3': 24, 'MPG_1': 22})
* Most likely due to errors in bids validation
* Fixed subjects:
    - SWU_3: added missing subject to participants.tsv
    - NYU_2: updated phase encoding direction in T1w.json
    - DC_1: set repetition time to 1s to make it consistent with pixdim in bold image
    - IPCAS_7: added missing subjects to participants.tsv
    - BNU_2: heterogeneous TRs, DON'T KNOW HOW TO FIX, blacklist
    - MRN: removed col 2 of participants.tsv, added missing participant
    - LMU_2: HETEROGENEOUS TRs, blacklist
    - IBA_TRT: HETEROGENEOUS TRs, blacklist
    - IPCAS_2: added missing participant to participants.tsv
    - LMU_1: HETEROGENEOUS TRs, blacklist
    - XHCUMS: added missing participant to participants.tsv
    - NKI_TRT: don't know how to fix, blacklist
    - SWU_4: removed dwi file with missing bvecs/bvals, added missing participant
      to participants.tsv
    - MPG_1: unable to read nifit header, blaklisted

* blacklist: UPSM_1, BNU_2, LMU_2, IBA_TRT, LMU_1, NKI_TRT, MPG_1
* put them in data/blacklisted
* all remaining subjects pass bids validation

* only the T1s are processed, it's not a lot of data
* the singularity image (420M) must be read by every task

# isilon-rep2, 92 jobs

* Should be done by midnight on Sep 1.
* Don't forget to cite GNU parallel:  
  O. Tange (2011): GNU Parallel - The Command-Line Power Tool,
  ;login: The USENIX Magazine, February 2011:42-47.
* forgot to clear the page cache
* why am I using 92 jobs while there are 96 cores?

# isilon-rep3, 96 jobs
# isilon-rep4, 96 jobs
# isilon-rep5, 96 jobs
# isilon-rep6, 25 jobs

rep3-6 worked fine

# isilon-rep7, 25 jobs

killed, will try the new async mode for isilon 

# isilon-rep8, 25 jobs

cancelled

# UPDATE: previous conditions should be discarded

We will add local disk (150G) and in-memory only has 298GB
So excluded the following datasets:

BMB_1  BNU_1  BNU_3  DC_1  HNU_1  SWU_1  SWU_4  UM  XHCUMS

Input data is now 141GB

# /run/user/61218 -- see launch-in-mem.sh
* in-mem-1, 96 jobs
* in-mem-2, 96 jobs
* in-mem-3, 96 jobs
* in-mem-4, 25 jobs
* in-mem-5, 25 jobs
* in-mem-6, 25 jobs
ALL DONE

Not sure why, but input data is 112GB on run/user, while it was 141GB on isilon

# disk -- see launch-disk.sh
Input data is 112GB here too.

bandwidth: 500 MB/s (?)
gamma: 96

D = 4M 
C = 22s

-> we're in page cache for both conditions

* disk-1: 96 jobs
* disk-4: 25 jobs

# nvme: /nvme-disk1 -- see launch-nvme.sh
* nvme-1: 96 jobs
* nvme-4: 25 jobs


# Methods
Removed output data after each run
Removed input data from volume before switching conditions

Infra:
* disk used xfs
* nvme used fuseblk
* run/user used tmpfs

# Conclusion from first glimpse

disk, in-mem and nvme give similar results, due to page cache.