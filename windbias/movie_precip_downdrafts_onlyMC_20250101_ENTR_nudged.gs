#! /bin/bash
#SBATCH --job-name=png_to_gif
        # Assigns the specified name to the request


cd /perm/paaa/images/temp_for_movies

# filename="map_downdrafts_onlyMC_ENTR_nudged_20250101"
# gifname="map_precip_tropics_2dryMF_20250101"
filename="map_p91_975hPa_20250101"
gifname="map_p91_975hPa_20250101"


for k in $(seq 0 1 9)
do
mv ${filename}_${k}.png ${filename}_0${k}.png
mv ${filename}_0${k}.png ${filename}_00${k}.png
done

for k in $(seq 10 1 99)
do
mv ${filename}_${k}.png ${filename}_0${k}.png
done


# -trim possible if you want to cut the white away

rm ${filename}_*_trim.png

#loop over all files
for i in $(seq 0 1 119)
do
printf -v i "%03d" $i
convert ${filename}_${i}.png -trim -bordercolor White -border 20x10 +repage ${filename}_${i}_trim.png
done


convert -delay 20 ${filename}_???_trim.png ${filename}_trim.gif

mv ${filename}_trim.gif /perm/paaa/movies/${gifname}_trim.gif
