docker build -t pgk_test:latest .

docker run --name my-test-server \
  -p 127.0.0.1:50051:50051 \
  -v /:/host_root \
  pgk_test:latest

# add -d to run in the. background
