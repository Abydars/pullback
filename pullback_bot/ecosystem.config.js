module.exports = {
  apps: [
    {
      name: "pullback-bot",
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
