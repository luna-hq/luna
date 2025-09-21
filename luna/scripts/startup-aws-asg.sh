#!/bin/bash

METADATA_TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INTERNAL_IP=$(curl -H "X-aws-ec2-metadata-token: $METADATA_TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)
mkdir -p /etc/luna/ && echo -n "$INTERNAL_IP" > /etc/luna/internal-ip

VERSION=$(curl -s https://api.github.com/repos/flowerinthenight/luna/releases/latest | jq -r ".tag_name")
cd /tmp/ && wget https://github.com/flowerinthenight/luna/releases/download/$VERSION/luna-$VERSION-x86_64-linux.tar.gz
tar xvzf luna-$VERSION-x86_64-linux.tar.gz
cp -v luna /usr/local/bin/luna
chown root:root /usr/local/bin/luna

cat >/usr/lib/systemd/system/luna.service <<EOL
[Unit]
Description=Luna in-memory OLAP SQL server

[Service]
Type=simple
Restart=always
RestartSec=10
ExecStart=/usr/bin/sh -c "RUST_LOG=info /usr/local/bin/luna"

[Install]
WantedBy=multi-user.target
EOL

systemctl daemon-reload
systemctl enable luna
systemctl start luna
systemctl status luna
