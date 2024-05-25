
# 1BRC-challenge

This is based on the challenge here: https://github.com/gunnarmorling/1brc/.

> The One Billion Row Challenge (1BRC) is a fun exploration of how far modern Java can be pushed for aggregating one billion rows from a text file.
> Grab all your (virtual) threads, reach out to SIMD, optimize your GC, or pull any other trick, and create the fastest implementation for solving this task!

---

## Generate test data

```sh
sudo apt install openjdk-21-jdk

cd ..
git clone https://github.com/gunnarmorling/1brc.git
cd 1brc
./mvnw clean verify
./create_measurements.sh 1000000000
time ./calculate_average_baseline.sh
```


## Iterations
```
time python3 0_py_wc/main.py

1000000000
0_py_simple/main.py  60,65s user 1,71s system 99% cpu 1:02,36 total
```

```
time pypy 0_py_wc/main.py

1000000000
pypy 0_py_wc/main.py  27,02s user 2,06s system 99% cpu 29,075 total
```
