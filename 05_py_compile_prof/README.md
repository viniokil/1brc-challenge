
## Install `mise`
```
sudo install -dm 755 /etc/apt/keyrings
wget -qO - https://mise.jdx.dev/gpg-key.pub | gpg --dearmor | sudo tee /etc/apt/keyrings/mise-archive-keyring.gpg 1> /dev/null
echo "deb [signed-by=/etc/apt/keyrings/mise-archive-keyring.gpg arch=amd64] https://mise.jdx.dev/deb stable main" | sudo tee /etc/apt/sources.list.d/mise.list
sudo apt update
sudo apt install -y mise
```

```sh
mise install python@3.12

mise use --global python@3.12

python3 --version
> Python 3.12.3

time python3 04_py_calculate_average/main.py > 04_py_calculate_average_out.txt
> python3 04_py_calculate_average/main.py > 04_py_calculate_average_out.txt  413,27s user 1,58s system 99% cpu 6:54,86 total

time python3 04_py_calculate_average/main.py > 04_py_calculate_average_out.txt
> python3 04_py_calculate_average/main.py > 04_py_calculate_average_out.txt  409,48s user 1,64s system 99% cpu 6:51,13 total
```
