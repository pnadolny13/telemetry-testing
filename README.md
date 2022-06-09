## Telemetry Test Script

```bash
cd meltano_project/telemetry_testing
meltano install
```

# Start Docker Snowplow Micro
cd [YOUR_PATH]/meltano/src/meltano/core/tracking

docker run   --mount type=bind,source=$(pwd),destination=/config   -p 9090:9090   snowplow/snowplow-micro:1.2.1   --collector-config /config/micro.conf   --iglu /config/iglu.json

# Update Path

- Update the meltano_path variable in `src/telemetry_test.py` to point to your meltano clone
