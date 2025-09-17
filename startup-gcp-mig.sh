#!/bin/sh

INTERNAL_IP=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
mkdir -p /etc/luna/ && echo -n "$INTERNAL_IP" > /etc/luna/internal-ip

VERSION=$(curl -s https://api.github.com/repos/flowerinthenight/luna/releases/latest | jq -r ".tag_name")
cd /tmp/ && wget https://github.com/flowerinthenight/luna/releases/download/$VERSION/luna-$VERSION-x86_64-linux.tar.gz
tar xvzf luna-$VERSION-x86_64-linux.tar.gz
cp -v luna /usr/local/bin/luna
chown root:root /usr/local/bin/luna

cat >/usr/lib/systemd/system/luna.service <<EOL
[Unit]
Description=Luna - in-memory, columnar SQL server

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
