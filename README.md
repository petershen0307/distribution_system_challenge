# distribution_system_challenge

[page](https://fly.io/dist-sys/)

## Prepare Maelstrom

### install open jdk with asdf

```bash
asdf plugin-add java https://github.com/halcyon/asdf-java.git
asdf install java openjdk-21
asdf global java openjdk-21
```

### install graphviz gnuplot

```bash
sudo apt install graphviz gnuplot
```

## Debugging maelstrom

``` bash
./maelstrom serve
```

You can then open a browser to `http://localhost:8080` to see results.
