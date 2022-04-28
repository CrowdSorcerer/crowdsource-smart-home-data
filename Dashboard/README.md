### Build docker for grafana

```bash
docker build ./Dashboard/Grafana
```

### Run grafana
```bash
docker run -d -p 80:3000 --name=grafana grafana/grafana-oss
```

### Run frontend
```bash
docker-compose up --build
```