### Build docker for grafana

```bash
docker build ./Dashboard/Grafana
```

```bash
docker run -d -p 80:3000 --name=grafana grafana/grafana-oss
```