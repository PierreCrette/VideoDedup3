# VIDEO DeDup

Find duplicate videos by content.
Parse a video directory to create one image every n seconds, then identify duplicate images and show possible video duplicates for manual analysis.

Licenced under GPL-3.0 except for ffmpeg, Imagemagick and Postgres who have their own licences.



# Version 3

Version published the 14th of February 2021 is considered as final. 



# Prerequisites

ffmpeg, imagemagick and python3 with some modules must be installed on your computer.

They are all included in Ubuntu distro (sudo apt-get install ffmpeg)

You have a /video folder.
You want to work in a /img folder.

This /img folder will include a /img/db to replicate /video structure and store jpeg images.
This /img folder will alse include a /img/ana-not-saved to copy duplicate files for analysis (remove it from your backup plan).
And also an /img/unwanted to remove imgages and pairs of videos from subsequent searchs.

This v3 wasn't tested in distributed computing mode. It should work but some bugs are probable.



# Options and parameters

The program contains help. Execute without parameter to display it.




# Step 1

Scan your video files and create for each file a folder image. In this image folder python calls ffmpeg to create 1 jpeg image every n seconds then it calculate fingerprint of each image (32 pixels wide x 18 pixels high x3 colors. Range from 0 to 1728) and store them in timages table. See findimagedupes documentation for algorythm since I copied it.



# Step 2

Parse the timages table and compare all and store set of duplicates.

Here some optimizations have been done:

First images distance from 4 origin keys arbitrary choosen limits the possible correspondances. Origin00 is a key=0. Then search of images distant of less than 20 from imageA who have dist00=100 (ie 100 bits set to 1) can be limited to images with 80<=dist00<=120. Limiting also against dist01, dist02 and dist03 restrict again the selection.

Second the order of comparizon (order by dist00, dist01, dist02, dist03) enable to avoid testing backward. Then only images with 100<=dist00<=120 in our example. This works only if no new images are added. 

IMPORTANT: Do the 1st step with a maximum of data before starting step 2. Then finish step 2 before launching step 1 anew. The difference is working with half power 4 data, ie 16 time less. Without this optimization the RAM usage can also constraint your computing.



# Step 3

Scan duplicates, remove some false duplicates (same image 2 times in the same video file), group duplicates by video pairs, and copy duplicates in the /img/ana-not-saved analyse folder.

You will have a lot of manual work to identify images not relevant (logos...) and copy them to -uw folder.


When identifying a duplicate you may put the source in a 'duplicate' folder of your source and later use a binary duplicate finder to remove them.



Examples :

1st run(s) to clean up the resultset file, erase already known unwanted images and remove results on deleted source files. -nosrccp option to limit disk workload :

./VideoDedup.py -src=/zpool/zdata/dvgrab -img=/mnt/mp400/img02/db -tmp=/mnt/mp600/tmp -sqldb=VideoDedup02 -rs=/mnt/mp400/img02/rs -uw=/mnt/mp400/img02/0uw -v=1 -fps=1 -maxdiff=7 -maxdiffuw=7 -minimg=5 -step=3 -speed=2 -threads=60 -nosrccp



2nd run(s) to remove some duplicates (few false positives) :

./VideoDedup.py -src=/zpool/zdata/dvgrab -img=/mnt/mp400/img02/db -tmp=/mnt/mp600/tmp -sqldb=VideoDedup02 -rs=/mnt/mp400/img02/rs -uw=/mnt/mp400/img02/0uw -v=1 -fps=1 -maxdiff=10 -maxdiffuw=7 -minimg=15 -step=3 -speed=2 -threads=60



3rd run(s) to remove some duplicates (more false positives). Decrease -t and increase -maxdiff by small increment if your resultset is important :

./VideoDedup.py -src=/zpool/zdata/dvgrab -img=/mnt/mp400/img02/db -tmp=/mnt/mp600/tmp -sqldb=VideoDedup02 -rs=/mnt/mp400/img02/rs -uw=/mnt/mp400/img02/0uw -v=1 -fps=1 -maxdiff=20 -maxdiffuw=7 -minimg=5 -step=3 -speed=2 -threads=60

    

# HOW TO

How to setup: read above description to understand then modify videodedup.sh to set your own folders.



## Initial run

Some parameters shall not be changed. Since the total operations will last days or weeks you should choose them correctly.

Average precision (less than a week?): -fps=10 to 60; -maxdiff=10

Good precision (Multiple weeks): -fps=2 to 10; -maxdiff=15

Very good precision: -fps=0.5 to 1; -maxdiff=20



## Maintenance run

Use same parameters than initial run. 



## Remove duplicate

This is out of the scope of this program.

Just move your certified duplicates to a /video/duplicates folder. Then run any dedup program (based on exact binary) to remove BOTH duplicate : your copy in /video/duplicates and the original.



## Remove false positives

Use case : same generic images present in different videos.
Copy the list of jpeg contained in /db/ana-not-saved into /db/unwanted for each set. The step 3 will discard them.

Use case : 2 video files not duplicate but some images are similar.
Copy the nb_match*.txt from /db/ana-not-saved to /db/unwanted. The step3 will discard them.



## Optimizations

Use limited parameters to target a 80% duplicate finding.

Use your computer ressources carrefully. If possible use a tmp folder on a SSD, img folder on another, SQL database on a 3rd. Use threads in order to get the max of your CPU but keep a eye on RAM to avoid swapping and on SQL disk to avoid moving constraint from CPU to SQL. In such cases it's better to decrease thread count.
