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