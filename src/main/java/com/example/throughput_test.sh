# Kafka Throughput Test Script 
# HOW TO RUN:
# (1) chmod +x src/main/java/com/example/throughput_test.sh    [MAKE IT EXECUTABLE]
# (2) cp src/main/java/com/example/throughput_test.sh ~/Downloads/kafka_2.13-4.1.1/    [MOVE TO KAFKA DIR]
# (3) cd ~/Downloads/kafka_2.13-4.1.1/    [CD TO KAFKA DIR]
# (4) ./throughput_test.sh 2>&1 | tee results.txt          [RUN THE TEST]

TOPIC="prod-cons-topic"
BROKER="localhost:9092"
RECORD_SIZE=1000   # 1 KB per message

# Three consumer groups — each reads the full topic independently
GROUP_1="consumer-group-small"       # consumes 100K messages (matches small producer runs)
GROUP_2="consumer-group-large"       # consumes 1M messages, default fetch-size
GROUP_3="consumer-group-large-fetch" # consumes 1M messages, 1MB fetch-size

# Helper: reset a group's offset to earliest
reset_offset() {
  local group=$1
  echo "  -> Resetting offset for group: $group"
  bin/kafka-consumer-groups.sh \
    --bootstrap-server $BROKER \
    --group $group \
    --topic $TOPIC \
    --reset-offsets \
    --to-earliest \
    --execute
  sleep 2   # give the broker time to apply the reset before consuming
}

echo "============================================="
echo " Step 1: Creating topic '$TOPIC'"
echo "============================================="
bin/kafka-topics.sh --create \
  --topic $TOPIC \
  --bootstrap-server $BROKER \
  --partitions 1 \
  --replication-factor 1
echo ""

echo "============================================="
echo " PRODUCER TESTS (exponential throughput)"
echo "============================================="

# echo ""
# echo "--- Round 1: target 1,000 msg/s ---"
# /home/ziad-sallam/Downloads/kafka_2.13-4.2.0/bin/kafka-producer-perf-test.sh \
#   --topic $TOPIC \
#   --num-records 100000 \
#   --record-size $RECORD_SIZE \
#   --throughput 1000 \
#   --producer-props bootstrap.servers=$BROKER

# echo ""
# echo "--- Round 2: target 10,000 msg/s ---"
# /home/ziad-sallam/Downloads/kafka_2.13-4.2.0/bin/kafka-producer-perf-test.sh \
#   --topic $TOPIC \
#   --num-records 100000 \
#   --record-size $RECORD_SIZE \
#   --throughput 10000 \
#   --producer-props bootstrap.servers=$BROKER

# echo ""
# echo "--- Round 3: target 100,000 msg/s ---"
# /home/ziad-sallam/Downloads/kafka_2.13-4.2.0/bin/kafka-producer-perf-test.sh \
#   --topic $TOPIC \
#   --num-records 1000000 \
#   --record-size $RECORD_SIZE \
#   --throughput 200000 \
#   --producer-props bootstrap.servers=$BROKER

echo ""
echo "--- Round 4: unlimited throughput (hardware ceiling) ---"
/home/ziad-sallam/Downloads/kafka_2.13-4.2.0/bin/kafka-producer-perf-test.sh \
  --topic $TOPIC \
  --num-records 10000000 \
  --record-size $RECORD_SIZE \
  --throughput -1 \
  --producer-props bootstrap.servers=$BROKER

# echo ""
# echo "============================================="
# echo " CONSUMER TESTS — 3 independent groups"
# echo "============================================="

# echo ""
# echo "--- Group 1: '$GROUP_1' — 100K messages, default fetch-size ---"
# echo "(simulates a lightweight consumer reading a small batch)"
# reset_offset $GROUP_1
# /home/ziad-sallam/Downloads/kafka_2.13-4.2.0/bin/kafka-consumer-perf-test.sh \
#   --bootstrap-server $BROKER \
#   --topic $TOPIC \
#   --messages 100000 \
#   --group $GROUP_1 \
#   --reporting-interval 1000 \
#   --show-detailed-stats

# echo ""
# echo "--- Group 2: '$GROUP_2' — 1M messages, default fetch-size ---"
# echo "(simulates a standard consumer reading the full topic)"
# reset_offset $GROUP_2
# /home/ziad-sallam/Downloads/kafka_2.13-4.2.0/bin/kafka-consumer-perf-test.sh \
#   --bootstrap-server $BROKER \
#   --topic $TOPIC \
#   --messages 1000000 \
#   --group $GROUP_2 \
#   --reporting-interval 1000 \
#   --show-detailed-stats

# echo ""
# echo "--- Group 3: '$GROUP_3' — 1M messages, 1MB fetch-size ---"
# echo "(simulates a high-throughput consumer with larger fetch batches)"
# reset_offset $GROUP_3
# /home/ziad-sallam/Downloads/kafka_2.13-4.2.0/bin/kafka-consumer-perf-test.sh \
#   --bootstrap-server $BROKER \
#   --topic $TOPIC \
#   --messages 1000000 \
#   --fetch-size 1048576 \
#   --group $GROUP_3 \
#   --reporting-interval 1000 \
#   --show-detailed-stats

# # =============================================================
# echo ""
# echo " All tests complete."
# echo " Groups used:"
# echo "   $GROUP_1  — lightweight (100K msgs, default fetch)"
# echo "   $GROUP_2  — standard   (1M msgs,   default fetch)"
# echo "   $GROUP_3  — optimized  (1M msgs,   1MB fetch)"
