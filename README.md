# CloudComputingA1

Logging into Spartan: ssh your-unimelb-username@spartan.hpc.unimelb.edu.au

The files to be used in this assignment are accessible at:
• /data/gpfs/projects/COMP90024/twitter-100gb.json
o this is the main file to use for your final analysis and report write up, i.e., do not use this file for
software development and testing. It is 120Gb of JSON and will take a long time to process.
• /data/gpfs/projects/COMP90024/twitter-50mb.json
o is a smaller (59Mb) JSON file that should be used for testing.
• /data/gpfs/projects/COMP90024/twitter-1mb.json
o is a small (1.2Mb) JSON file that should be used for initial testing.

You should make a symbolic link to these files on SPARTAN, i.e., you should run the following commands at the Unix
prompt from your own user directory on SPARTAN:
ln –s /data/gpfs/projects/COMP90024/twitter-100gb.json
ln –s /data/gpfs/projects/COMP90024/twitter-50mb.json
ln –s /data/gpfs/projects/COMP90024/twitter-1mb.json

Your assignment is to (eventually!) search the large Twitter data file and identify:
• the happiest hour ever, e.g. 3-4pm on 23rd November with an overall sentiment score of +12,
• the happiest day ever, e.g. 25th February was the happiest day with an overall sentiment score of +23,
• the most active hour ever, e.g. 4-5pm on 3rd March had the most tweets (#1234),
• the most active day ever, e.g. 3rd October had the most tweets (#12345)
