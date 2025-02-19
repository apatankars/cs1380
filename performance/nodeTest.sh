#!/bin/bash

NODES=(
'{"type":"object","value":{"ip":"{\"type\":\"string\",\"value\":\"127.0.0.1\"}","port":"{\"type\":\"number\",\"value\":\"9007\"}","onStart":"{\"type\":\"function\",\"value\":\"(s) => console.log(\\\"Node started at 9007\\\")\"}"}}'
'{"type":"object","value":{"ip":"{\"type\":\"string\",\"value\":\"127.0.0.1\"}","port":"{\"type\":\"number\",\"value\":\"9090\\")","onStart":"{\"type\":\"function\",\"value\":\"(s) => console.log(\\\"Node started at 9090\\\")\"}"}}'
'{"type":"object","value":{"ip":"{\"type\":\"string\",\"value\":\"127.0.0.1\"}","port":"{\"type\":\"number\",\"value\":\"8002\"}","onStart":"{\"type\":\"function\",\"value\":\"(s) => console.log(\\\"Node started at 8002\\\")\"}"}}'
'{"type":"object","value":{"ip":"{\"type\":\"string\",\"value\":\"127.0.0.1\"}","port":"{\"type\":\"number\",\"value\":\"8003\"}","onStart":"{\"type\":\"function\",\"value\":\"(s) => console.log(\\\"Node started at 8003\\\")\"}"}}'
'{"type":"object","value":{"ip":"{\"type\":\"string\",\"value\":\"127.0.0.1\"}","port":"{\"type\":\"number\",\"value\":\"8004\"}","onStart":"{\"type\":\"function\",\"value\":\"(s) => console.log(\\\"Node started at 8004\\\")\"}"}}'
'{"type":"object","value":{"ip":"{\"type\":\"string\",\"value\":\"127.0.0.1\"}","port":"{\"type\":\"number\",\"value\":\"8005\"}","onStart":"{\"type\":\"function\",\"value\":\"(s) => console.log(\\\"Node started at 8005\\\")\"}"}}'
)

latencies=()
start_time=$(date +%s%3N)

echo "Starting nodes..."
for NODE in "${NODES[@]}"; do
  node_start=$(date +%s%3N)
  # Create a temporary file for capturing the node output
  tmp_file=$(mktemp)

  # Start the node, redirecting stdout and stderr to the temp file
  ./distribution.js --config "$NODE" > "$tmp_file" 2>&1 &
  pid=$!

  # Wait until we see the onStart message in the output.
  # Adjust the grep pattern if your message changes.
  while ! grep -q "Node started" "$tmp_file"; do
    sleep 0.05
  done

  node_end=$(date +%s%3N)
  latency=$((node_end - node_start))
  latencies+=($latency)
  echo "Node started in ${latency}ms"

  # Optionally, kill the node process since we only want to measure boot time
  kill $pid 2>/dev/null
  rm "$tmp_file"
done

end_time=$(date +%s%3N)
total_time=$((end_time - start_time))
total_nodes=${#NODES[@]}

# Calculate average latency
sum=0
for lat in "${latencies[@]}"; do
  sum=$((sum + lat))
done
avg_latency=$((sum / total_nodes))

# Calculate throughput
throughput=$(echo "scale=2; $total_nodes / ($total_time / 1000)" | bc)

echo -e "\nPerformance Metrics:"
echo "Average Latency: $avg_latency ms"
echo "Throughput: $throughput nodes/sec"