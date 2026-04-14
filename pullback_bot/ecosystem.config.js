const fs = require('fs');

let appName = "pullback-bot";

try {
  if (fs.existsSync('.env')) {
    const envFile = fs.readFileSync('.env', 'utf8');
    const match = envFile.match(/^PM2_APP_NAME=(.*)$/m);
    if (match && match[1]) {
      appName = match[1].trim().replace(/^["']|["']$/g, '');
    }
  }
} catch (e) {
  // Ignored
}

// Command-line env variables take priority if passed directly
appName = process.env.PM2_APP_NAME || appName;

module.exports = {
  apps: [
    {
      name: appName,
      script: "main.py",
      interpreter: "./venv/bin/python",
      watch: false,
      autorestart: true,
      max_restarts: 20,
      min_uptime: "10s",
      restart_delay: 3000,
      exp_backoff_restart_delay: 100,
      max_memory_restart: "8G",
      env: {
        PYTHONUNBUFFERED: "1"
      },
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      error_file: "logs/error.log",
      out_file: "logs/out.log",
      merge_logs: true
    }
  ]
};
