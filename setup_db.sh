#!/bin/bash

# PostgreSQL setup script
echo "Setting up PostgreSQL database for TCP Router..."

# Create database if it doesn't exist
sudo -u postgres psql <<EOF
CREATE DATABASE tcp_router;
CREATE USER tcp_router_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE tcp_router TO tcp_router_user;
\c tcp_router
GRANT ALL ON SCHEMA public TO tcp_router_user;
EOF

echo "Database created successfully!"
echo ""
echo "Update your config.toml with:"
echo "user = 'tcp_router_user'"
echo "password = 'your_secure_password'"