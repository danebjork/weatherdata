for f in *.tar.gz
do 
  mkdir "${f%.tar.gz}"
  tar zxvf "$f" -C "${f%.tar.gz}"/. 
done

for f in *.zip
do
  tmp=${f#QCLCD}
  mkdir "${tmp%.zip}"
  unzip "$f" -d "${tmp%.zip}"/.
done 

rm *.zip
rm *.tar.gz
rm *dailyavg.txt
rm *hpd.txt

./rename.sh
