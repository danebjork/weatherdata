for f in *hourly.txt
do
  mv "$f" "${f%.txt}.csv"
done
 
for f in *daily.txt
do
  mv "$f" "${f%.txt}.csv"
done


for f in *monthly.txt 
do
  mv "$f" "${f%.txt}.csv"
done

