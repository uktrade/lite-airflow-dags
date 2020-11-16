FROM alpine:3
RUN apk add --no-cache openssh-client
CMD ssh-add -l && ssh \
  -v \
  -o StrictHostKeyChecking=no \
  -o ConnectTimeout=5 \
  -o ServerAliveInterval=30 \
  -o KexAlgorithms=ecdh-sha2-nistp521 \
  -N ec2-user@$TUNNEL_HOST \
  -L *:$LOCAL_PORT:$REMOTE_HOST:$REMOTE_PORT
